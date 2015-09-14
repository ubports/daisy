#!/usr/bin/python
# -*- coding: utf-8 -*-
# 
# Copyright © 2011-2014 Canonical Ltd.
# Authors: Evan Dandrea <evan.dandrea@canonical.com>
#          Brian Murray <brian.murray@canonical.com>
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero Public License as published by
# the Free Software Foundation; version 3 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero Public License for more details.
# 
# You should have received a copy of the GNU Affero Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import amqplib.client_0_8 as amqp
import apport
import argparse
import atexit
import datetime
import logging
import oops_dictconfig
import os
import re
import shutil
import socket
import sys
import tempfile
import traceback
import time

from subprocess import Popen, PIPE

from pycassa import ConsistencyLevel
from pycassa.columnfamily import ColumnFamily
from pycassa.cassandra.ttypes import NotFoundException
from pycassa.pool import MaximumRetryException
from pycassa.types import IntegerType, FloatType, UTF8Type

from daisy.metrics import wrapped_connection_pool, get_metrics, record_revno
from daisy import utils
from daisy.version import version_info
from daisy import config
from oopsrepository import config as oopsconfig

LOGGING_FORMAT = ('%(asctime)s:%(process)d:%(thread)d'
                  ':%(levelname)s:%(name)s:%(message)s')

_cached_swift = None
_cached_s3 = None

metrics = get_metrics('retracer.%s' % socket.gethostname())

def log(message, level=logging.INFO):
    logging.log(level, message)

def rm_eff(path):
    '''Remove ignoring -ENOENT.'''
    try:
        os.remove(path)
    except OSError, e:
        if e.errno != 2:
            raise

@atexit.register
def shutdown():
    log('Shutting down.')
    metrics.meter('shutdown')

def prefix_log_with_amqp_message(func):
    def wrapped(obj, msg):
        try:
            # This is a terrible hack to include the UUID for the core file and
            # OOPS report as well as the storage provider name with the log
            # message.
            format_string = ('%(asctime)s:%(process)d:%(thread)d:%(levelname)s'
                             ':%(name)s:' + msg.body + ':%(message)s')
            formatter = logging.Formatter(format_string)
            logging.getLogger().handlers[0].setFormatter(formatter)
            func(obj, msg)
        finally:
            formatter = logging.Formatter(LOGGING_FORMAT)
            logging.getLogger().handlers[0].setFormatter(formatter)
    return wrapped

def chunked_insert(cf, row_key, data):
    # The thrift_framed_transport_size_in_mb limit is 15 MB by default, but
    # there seems to be some additional overhead between 64 and 128 bytes.
    # max_size = (1024 * 1024 * 15 - 128)
    # Production doesn't seem to like 15 MB, so lets play it conservatively.
    max_size = (1024 * 1024 * 4)

    for key in data:
        val = data[key]
        if len(val) > max_size:
            riter = reversed(range(0, len(val), max_size))
            res = [val[i:i+max_size] for i in riter]
            i = 0
            for r in reversed(res):
                params = (len(r), key, i, row_key)
                log('Inserting chunk of size %d into %s-%d for %s' % params)
                if i == 0:
                    cf.insert(row_key, {key: r})
                else:
                    cf.insert(row_key, {'%s-%d' % (key, i): r})
                i += 1
        else:
            cf.insert(row_key, {key: data[key]})

class ApportException(Exception):
    """Class for exceptions caused by apport retrace"""
    pass


class Retracer:
    def __init__(self, config_dir, sandbox_dir, architecture, verbose,
                 cache_debs, use_sandbox, cleanup_sandbox, cleanup_debs,
                 failed=False):
        self.setup_cassandra()
        self.config_dir = config_dir
        self.sandbox_dir = sandbox_dir
        self.verbose = verbose
        self.architecture = architecture
        self.failed = failed
        # A mapping of release names to temporary sandbox and cache
        # directories, so that we can remove them at the end of the run.
        # TODO: we should create a single temporary directory that all of these
        # live under, saving the multiple calls to atexit.register.
        self._sandboxes = {}
        # The time we were last able to talk to the AMQP server.
        self._lost_connection = None
        self.cache_debs = cache_debs
        self.use_sandbox = use_sandbox
        self.cleanup_sandbox = cleanup_sandbox
        self.cleanup_debs = cleanup_debs

        # determine path of gdb
        gdb_which = Popen(['which', 'gdb'], stdout=PIPE,
                          universal_newlines=True)
        self.gdb_path = gdb_which.communicate()[0].strip()

        # determine path of apport-retrace
        which = Popen(['which', 'apport-retrace'], stdout=PIPE,
                      universal_newlines=True)
        self.apport_retrace_path = which.communicate()[0].strip()

        m = 'Cannot find apport-retrace in $PATH (%s)' % os.environ.get('PATH')
        assert which.returncode == 0, m

    def setup_cassandra(self):
        os.environ['OOPS_KEYSPACE'] = config.cassandra_keyspace
        self.oops_config = oopsconfig.get_config()
        self.oops_config['host'] = config.cassandra_hosts
        self.oops_config['username'] = config.cassandra_username
        self.oops_config['password'] = config.cassandra_password
        self.oops_config['pool_size'] = config.cassandra_pool_size
        self.oops_config['max_overflow'] = config.cassandra_max_overflow

        self.pool = wrapped_connection_pool('retracer')
        self.oops_cf = ColumnFamily(self.pool, 'OOPS')
        self.indexes_fam = ColumnFamily(self.pool, 'Indexes')
        self.stacktrace_cf = ColumnFamily(self.pool, 'Stacktrace')
        self.awaiting_retrace_fam = ColumnFamily(self.pool, 'AwaitingRetrace')
        # Retry counter increments. This may result in double counting, but
        # we'd end up risking that anyway if failing with a timeout exception
        # and then re-rerunning the retrace later.
        self.retrace_stats_fam = ColumnFamily(self.pool, 'RetraceStats',
                                              retry_counter_mutations=True)
        self.bucket_fam = ColumnFamily(self.pool, 'Bucket')
        self.bucketretracefail_fam = ColumnFamily(self.pool,
            'BucketRetraceFailureReason')

        # We didn't set a default_validation_class for these in the schema.
        # Whoops.
        self.oops_cf.default_validation_class = UTF8Type()
        self.indexes_fam.default_validation_class = UTF8Type()
        self.stacktrace_cf.default_validation_class = UTF8Type()
        self.awaiting_retrace_fam.default_validation_class = UTF8Type()

    def listen(self):
        if self.failed:
            retrace = 'failed_retrace_%s'
        else:
            retrace = 'retrace_%s'
        retrace = retrace % self.architecture
        connection = None
        channel = None
        try:
            if config.amqp_username and config.amqp_password:
                connection = amqp.Connection(host=config.amqp_host,
                                        userid=config.amqp_username,
                                        password=config.amqp_password)
            else:
                connection = amqp.Connection(host=config.amqp_host)
            channel = connection.channel()
            channel.queue_declare(queue=retrace, durable=True,
                                  auto_delete=False)
            channel.basic_qos(0, 1, False)
            log('Waiting for messages. ^C to exit.')
            self.run_forever(channel, self.callback, queue=retrace)
        finally:
            if connection:
                connection.close()
            if channel:
                channel.close()

    def run_forever(self, channel, callback, queue):
        tag = channel.basic_consume(callback=callback, queue=queue)
        try:
            while (not self._lost_connection or
                   time.time() < self._lost_connection + 120):
                try:
                    channel.wait()
                except (socket.error, IOError), e:
                    is_amqplib_ioerror = (type(e) is IOError and
                                          e.args == ('Socket error',))
                    amqplib_conn_errors = (socket.error,
                                           amqp.AMQPConnectionException)
                    is_amqplib_conn_error = isinstance(e, amqplib_conn_errors)
                    if is_amqplib_conn_error or is_amqplib_ioerror:
                        self._lost_connection = time.time()
                        log('lost connection to Rabbit')
                        metrics.meter('lost_rabbit_connection')
                        # Don't probe immediately, give the network/process
                        # time to come back.
                        time.sleep(0.1)
                    else:
                        raise
            log('Rabbit did not reappear quickly enough.')
        except KeyboardInterrupt:
            pass
        if channel and channel.is_open:
            channel.basic_cancel(tag)

    def update_retrace_stats(self, release, day_key, retracing_time,
                             success=True, crashed=False):
        """
        release: the distribution release, ex. 'Ubuntu 12.04'
        day_key: the date as a YYYYMMDD string.
        retracing_time: the amount of time it took to retrace.
        success: whether crash was retraceable.
        """
        # This is kept around for legacy reasons. The integration tests
        # currently depend on this being exposed in the API.
        if crashed:
            status = 'crashed'
        elif not success:
            status = 'failed'
        else:
            status = 'success'
        # We can't mix counters and other data types
        self.retrace_stats_fam.add(day_key, '%s:%s' % (release, status))

        # Compute the cumulative moving average
        mean_key = '%s:%s:%s' % (day_key, release, self.architecture)
        count_key = '%s:count' % mean_key
        self.indexes_fam.column_validators = {mean_key: FloatType(),
                                              count_key: IntegerType()}
        try:
            mean = self.indexes_fam.get('mean_retracing_time',
                    column_start=mean_key, column_finish=count_key)
        except NotFoundException:
            mean = {mean_key: 0.0, count_key: 0}

        new_mean = float((retracing_time + mean[count_key] * mean[mean_key]) /
                          (mean[count_key] + 1))
        mean[mean_key] = new_mean
        mean[count_key] += 1
        self.indexes_fam.insert('mean_retracing_time', mean)

        # Report this into statsd as well.
        prefix = 'timings.retracing'
        if release:
            m = '%s.all_releases.%s.%s' % (prefix, self.architecture, status)
            metrics.timing(m, retracing_time)
            m = '%s.%s.all_architectures.%s' % (prefix, release, status)
            metrics.timing(m, retracing_time)
            m = '%s.%s.%s.%s' % (prefix, release, self.architecture, status)
            metrics.timing(m, retracing_time)
        m = '%s.all.all_architectures.%s' % (prefix, status)
        metrics.timing(m, retracing_time)
        m = '%s.%s.all.%s' % (prefix, self.architecture, status)
        metrics.timing(m, retracing_time)

    def setup_cache(self, sandbox_dir, release):
        if release in self._sandboxes:
            return self._sandboxes[release]
        sandbox_release = os.path.join(sandbox_dir, release)
        if not os.path.exists(sandbox_release):
            os.makedirs(sandbox_release)
        instance_sandbox = tempfile.mkdtemp(prefix='cache-', dir=sandbox_release)
        atexit.register(shutil.rmtree, instance_sandbox)
        # Write a pid file so that if we have to wipe out a cache that has
        # grown too large we can stop the retracer responsible for it before
        # doing so.
        with open(os.path.join(instance_sandbox, 'pid'), 'w') as fp:
            fp.write('%d' % os.getpid())
        sandbox = os.path.join(instance_sandbox, 'sandbox')
        os.mkdir(sandbox)
        cache = None
        if self.cache_debs:
            cache = os.path.join(instance_sandbox, 'cache')
            os.mkdir(cache)
        self._sandboxes[release] = (sandbox, cache)
        return self._sandboxes[release]

    def move_to_failed_queue(self, msg):
        if self.failed:
            # It's already on the failed queue.
            return

        # We've processed this. Delete it off the MQ.
        msg.channel.basic_ack(msg.delivery_tag)
        # We don't call self.processed here because that would remove the core
        # file from the storage provider, and we want to retain it.

        # Add it to the failed to retrace queue.
        queue = 'failed_retrace_%s' % self.architecture
        msg.channel.queue_declare(queue=queue, durable=True, auto_delete=False)
        body = amqp.Message(msg.body)
        # Persistent
        body.properties['delivery_mode'] = 2
        msg.channel.basic_publish(body, exchange='', routing_key=queue)

    def failed_to_process(self, msg, oops_id, old=False):
        processed = self.processed(msg)
        # Removing the core file failed in the processing phase, so requeue
        # the crash unless it is an old OOPS then don't requeue it.
        if not processed and not old:
            log('Requeued failed to process OOPS (%s)' % oops_id)
            self.requeue(msg, oops_id)
        # Also remove it from the retracing index, if we haven't already.
        try:
            addr_sig = self.oops_cf.get(oops_id,
                            ['StacktraceAddressSignature', 'SystemIdentifier'])
            addr_sig = addr_sig.values()[0]
            self.indexes_fam.remove('retracing', [addr_sig])
        except NotFoundException:
            log('Could not remove from the retracing row (%s):' % oops_id)

    def write_swift_bucket_to_disk(self, key, provider_data):
        global _cached_swift
        import swiftclient
        opts = {'tenant_name': provider_data['os_tenant_name'],
                'region_name': provider_data['os_region_name']}
        if not _cached_swift:
            _cached_swift = swiftclient.client.Connection(
                        provider_data['os_auth_url'],
                        provider_data['os_username'],
                        provider_data['os_password'],
                        os_options=opts,
                        auth_version='2.0')
        if self.verbose:
            log('swift token: %s' % str( _cached_swift.token))
        fmt = '-{}.{}.oopsid'.format(provider_data['type'], key)
        fd, path = tempfile.mkstemp(fmt)
        os.close(fd)
        bucket = provider_data['bucket']
        try:
            headers, body = _cached_swift.get_object(bucket, key,
                                                     resp_chunk_size=65536)
            with open(path, 'wb') as fp:
                for chunk in body:
                    fp.write(chunk)
            return path
        except swiftclient.client.ClientException:
            metrics.meter('swift_client_exception')
            log('Could not retrieve %s (swift):' % key)
            log(traceback.format_exc())
            # This will still exist if we were partway through a write.
            rm_eff(path)
            return None

    def remove_from_swift(self, key, provider_data):
        global _cached_swift
        import swiftclient
        opts = {'tenant_name': provider_data['os_tenant_name'],
                'region_name': provider_data['os_region_name']}
        try:
            if not _cached_swift:
                _cached_swift = swiftclient.client.Connection(
                            provider_data['os_auth_url'],
                            provider_data['os_username'],
                            provider_data['os_password'], os_options=opts,
                            auth_version='2.0')
            if self.verbose:
                log('swift token: %s' % str( _cached_swift.token))
            bucket = provider_data['bucket']
            _cached_swift.delete_object(bucket, key)
        except swiftclient.client.ClientException:
            log('Could not remove %s (swift):' % key)
            log(traceback.format_exc())
            metrics.meter('swift_delete_error')
            return False
        return True

    def write_s3_bucket_to_disk(self, key, provider_data):
        global _cached_s3
        from boto.s3.connection import S3Connection
        from boto.exception import S3ResponseError

        if not _cached_s3:
            _cached_s3 = S3Connection(
                    aws_access_key_id=provider_data['aws_access_key'],
                    aws_secret_access_key=provider_data['aws_secret_key'],
                    host=provider_data['host'])
        try:
            bucket = _cached_s3.get_bucket(provider_data['bucket'])
            key = bucket.get_key(key)
        except S3ResponseError:
            log('Could not retrieve %s (s3):' % key)
            log(traceback.format_exc())
            return None
        fmt = '-{}.{}.oopsid'.format(provider_data['type'], key)
        fd, path = tempfile.mkstemp(fmt)
        os.close(fd)
        with open(path, 'wb') as fp:
            for data in key:
                # 8K at a time.
                fp.write(data)
        return path

    def remove_from_s3(self, key, provider_data):
        global _cached_s3
        from boto.s3.connection import S3Connection
        from boto.exception import S3ResponseError

        try:
            if not _cached_s3:
                _cached_s3 = S3Connection(
                    aws_access_key_id=provider_data['aws_access_key'],
                    aws_secret_access_key=provider_data['aws_secret_key'],
                    host=provider_data['host'])
            bucket = _cached_s3.get_bucket(provider_data['bucket'])
            key = bucket.get_key(key)
            key.delete()
        except S3ResponseError:
            log('Could not remove %s (s3):' % key)
            log(traceback.format_exc())
            return False
        return True

    def write_local_to_disk(self, key, provider_data):
        path = os.path.join(provider_data['path'], key)
        fmt = '-{}.{}.oopsid'.format(provider_data['type'], key)
        fd, new_path = tempfile.mkstemp(fmt)
        os.close(fd)
        if not os.path.exists(path):
            return None
        else:
            shutil.copyfile(path, new_path)
            return new_path

    def remove_from_local(self, key, provider_data):
        path = os.path.join(provider_data['path'], key)
        rm_eff(path)
        return True

    def write_bucket_to_disk(self, oops_id, provider):
        path = ''
        cs = getattr(config, 'core_storage', '')
        if not cs:
            log('core_storage not set.')
            sys.exit(1)
        provider_data = cs[provider]
        t = provider_data['type']
        if t == 'swift':
            path = self.write_swift_bucket_to_disk(oops_id, provider_data)
        elif t == 's3':
            path = self.write_s3_bucket_to_disk(oops_id, provider_data)
        elif t == 'local':
            path = self.write_local_to_disk(oops_id, provider_data)
        return path

    def remove(self, oops_id, provider):
        cs = getattr(config, 'core_storage', '')
        if not cs:
            log('core_storage not set.')
            sys.exit(1)
        provider_data = cs[provider]
        t = provider_data['type']
        if t == 'swift':
            removed = self.remove_from_swift(oops_id, provider_data)
        elif t == 's3':
            removed = self.remove_from_s3(oops_id, provider_data)
        elif t == 'local':
            removed = self.remove_from_local(oops_id, provider_data)
        if removed:
            return True
        return False

    @prefix_log_with_amqp_message
    def callback(self, msg):
        log('Processing.')
        parts = msg.body.split(':', 1)
        oops_id, provider = parts
        try:
            quorum = ConsistencyLevel.QUORUM
            col = self.oops_cf.get(oops_id, read_consistency_level=quorum)
        except NotFoundException:
            # We do not have enough information at this point to be able to
            # remove this from the retracing row in the Indexes CF. Throw it
            # back on the queue and hope that eventual consistency works its
            # magic by then.
            log('Unable to find in OOPS CF.')
            self.requeue(msg, oops_id)

            metrics.meter('could_not_find_oops')
            return
        # There are some items still in amqp queue that have already been
        # retraced, check for this and ack the message.
        if 'RetraceFailureReason' in col.keys():
            log("Ack'ing already retraced OOPS.")
            msg.channel.basic_ack(msg.delivery_tag)
            return

        path = self.write_bucket_to_disk(*parts)

        if not path or not os.path.exists(path):
            log('Could not find %s' % path)
            self.failed_to_process(msg, oops_id)
            return

        core_file = '%s.core' % path
        with open(core_file, 'wb') as fp:
            log('Decompressing to %s' % core_file)
            p1 = Popen(['base64', '-d', path], stdout=PIPE)
            # Set stderr to PIPE so we get output in the result tuple.
            p2 = Popen(['zcat'], stdin=p1.stdout, stdout=fp, stderr=PIPE)
            ret = p2.communicate()
        rm_eff(path)

        if p2.returncode != 0:
            log('Error processing %s:' % path)
            if ret[1]:
                for line in ret[1].splitlines():
                    log(line)
            # We couldn't decompress this, so there's no value in trying again.
            self.processed(msg)
            # probably incomplete cores from armhf?
            metrics.meter('retrace.failed')
            metrics.meter('retrace.failed.%s' %
                          self.architecture)
            metrics.meter('retrace.failure.decompression')
            metrics.meter('retrace.failure.decompression.%s' %
                          self.architecture)
            return
        # confirm that gdb thinks the core file is good
        gdb_cmd = [self.gdb_path, "--batch", "--ex", "target core %s" %
                   core_file]
        proc = Popen(gdb_cmd, stdout=PIPE, stderr=PIPE,
                     universal_newlines=True)
        (out, err) = proc.communicate()
        if 'is truncated: expected core file size' in err or \
                'not a core dump' in err:
            # Not a core file, there's no value in trying again.
            self.processed(msg)
            log('Not a core dump per gdb.')
            metrics.meter('retrace.failed')
            metrics.meter('retrace.failed.%s' %
                          self.architecture)
            metrics.meter('retrace.failure.gdb_core_check')
            metrics.meter('retrace.failure.gdb_core_check.%s' %
                          self.architecture)
            return

        report = apport.Report()

        for k in col:
            try:
                report[k.encode('UTF-8')] = col[k].encode('UTF-8')
            except AssertionError:
                # apport raises an AssertionError if a key is invalid
                # e.g. /usr/bin/media-hub-server became a key somehow,
                # and this doesn't need to be part of the report used
                # for retracing
                continue

        # these will not change after retracing
        architecture = report.get('Architecture', '')
        release = report.get('DistroRelease', '')
        bad = '[^-a-zA-Z0-9_.() ]+'
        retraceable = utils.retraceable_release(release)
        if not retraceable:
            metrics.meter('retrace.failed.notretraceable')
        package = report.get('Package', '')
        # there will not be a debug symbol version of the package
        if not utils.retraceable_package(package):
            log('Not retraced due to foreign origin.')
            metrics.meter('retrace.failed.foreign')
            retraceable = False
        invalid = re.search(bad, release) or len(release) > 1024
        if invalid:
            metrics.meter('retrace.failed.invalid')
        if not release or invalid or not retraceable:
            self.processed(msg)
            return

        report['CoreDump'] = (core_file,)
        report_path = '%s.crash' % path
        with open(report_path, 'wb') as fp:
            report.write(fp)

        try:
            log('Retracing {}'.format(msg.body))
            sandbox, cache = self.setup_cache(self.sandbox_dir, release)
            day_key = time.strftime('%Y%m%d', time.gmtime())

            retracing_start_time = time.time()
            # the easiest way to test not using a sandbox is to make it another
            # command line option like don't use sandbox even though we will
            # provide it on the cli
            cmd = ['python3', self.apport_retrace_path, report_path,
                   '--remove-core', '--sandbox', self.config_dir, '--output',
                   '%s.new' % report_path]
            if self.use_sandbox:
                cmd.extend(['--sandbox-dir', sandbox])
            if cache:
                cmd.extend(['-C', cache])
            if self.verbose:
                cmd.append('-v')
            # use our own crashdb config with all supported architectures
            env = os.environ.copy()
            env['APPORT_CRASHDB_CONF'] = os.path.join(self.config_dir, 'crashdb.conf')
            http_proxy = env.get('retracer_http_proxy')
            if http_proxy:
                env.update({'http_proxy': http_proxy})
            proc = Popen(cmd, env=env, stdout=PIPE, stderr=PIPE,
                         universal_newlines=True)
            out, err = proc.communicate()
        except:
            rm_eff('%s.new' % report_path)
            log('Failure in retrace set up for {}'.format(msg.body))
            log(traceback.format_exc())
            metrics.meter('retrace.failed')
            metrics.meter('retrace.failed.%s' % release)
            metrics.meter('retrace.failed.%s' % architecture)
            metrics.meter('retrace.failed.%s.%s' %
                          (release, architecture))
            metrics.meter('retrace.failed.to_setup')
            metrics.meter('retrace.failed.to_setup.%s' % release)
            metrics.meter('retrace.failed.to_setup.%s' % architecture)
            metrics.meter('retrace.failed.to_setup.%s.%s' %
                          (release, architecture))
            raise
        finally:
            if sandbox and self.cleanup_sandbox:
                shutil.rmtree(sandbox)
                os.mkdir(sandbox)
            if cache and self.cleanup_debs:
                shutil.rmtree(cache)
                os.mkdir(cache)
            rm_eff(report_path)

        try:
            if proc.returncode != 0:
                missing_pkg = False
                if proc.returncode == 1:
                    # Package download errors return 1
                    log("Apport's return code was 1.")
                    # Log the error from apport
                    retry = False
                    for std in (out, err):
                        for line in std.splitlines():
                            log(line)
                            # this happens for binaries from packages not in Ubuntu
                            if 'Cannot find package which ships ExecutablePath' \
                                    in line:
                                missing_pkg = True
                                break
                            if 'Package download error, try again later' \
                                    in line:
                                retry = True
                                break
                            # probably due to network errors
                            if 'W:GPG error:' in line:
                                retry = True
                                break
                            # probably due to network errors
                            if 'index files failed to download.' \
                                    in line:
                                retry = True
                                break
                    if retry:
                        log("Will retry (%s) due to a transient error." %
                            oops_id)
                        self.requeue(msg, oops_id)
                        # don't record it as a failure in the metrics as it is
                        # going to be retried
                        rm_eff('%s.new' % report_path)
                        # return immediately to prevent moving the crash to
                        # the failed queue
                        return
                elif proc.returncode == -15:
                    log("apport-retrace was killed by retracer restart.")
                    return
                # apport-retrace will exit 0 even on a failed retrace unless
                # something has gone wrong at a lower level, as was the case
                # when python-apt bailed out on invalid sources.list files.
                # Fail hard so we do not incorrectly write a lot of retraces to
                # the database as failures.
                retracing_time = time.time() - retracing_start_time
                invalid_core = False
                for std in (out, err):
                    for line in std.splitlines():
                        log(line)
                        if "Invalid core dump" in line:
                            invalid_core = True
                            break
                        # crash file may have been cleaned up from underneath
                        # us by retracer restart script
                        #elif "is neither an existing" in line:
                        #    cfile = line.split(" ")[1].strip('"')
                        #    if not os.path.exists(cfile):
                        #        log("Will retry this oops later.")
                        #        return
                m = 'Retrace failed (%i), %s'
                if missing_pkg:
                    action = 'leaving as failed.'
                    # we don't want to see this OOPS again so process it
                    self.processed(msg)
                else:
                    self.move_to_failed_queue(msg)
                    action = 'moving to failed queue.'
                log(m % (proc.returncode, action))
                if invalid_core:
                    # these should not be reported LP: #1354571 so record
                    # apport version
                    apport_vers = report.get('ApportVersion', '')
                    metrics.meter('retrace.failed.invalid_core')
                    metrics.meter('retrace.failed.invalid_core.%s' % release)
                    metrics.meter('retrace.failed.invalid_core.%s' %
                                  architecture)
                    metrics.meter('retrace.failed.invalid_core.%s.%s' %
                                  (release, architecture))
                    if apport_vers:
                        metrics.meter('retrace.failed.invalid_core.%s.%s'
                            % (release, apport_vers.replace('.', '_')))
                # Remove the SAS from the retracing index so that we ask for
                # another core
                sas = report.get('StacktraceAddressSignature', '')
                if sas:
                    self.indexes_fam.remove('retracing', [sas])
                self.update_retrace_stats(release, day_key, retracing_time,
                                          crashed=True)
                metrics.meter('retrace.failed')
                metrics.meter('retrace.failed.%s' % release)
                metrics.meter('retrace.failed.%s' % architecture)
                metrics.meter('retrace.failed.%s.%s' %
                              (release, architecture))
                rm_eff('%s.new' % report_path)
                raise ApportException(err)
                return

            retracing_time = time.time() - retracing_start_time

            if not os.path.exists('%s.new' % report_path):
                log('%s.new did not exist.' % report_path)
                metrics.meter('retrace.missing.retraced_crash_file')
                self.failed_to_process(msg, oops_id)
                metrics.meter('retrace.failed')
                metrics.meter('retrace.failed.%s' % release)
                metrics.meter('retrace.failed.%s' % architecture)
                metrics.meter('retrace.failed.%s.%s' %
                              (release, architecture))

                return

            log('Writing back to Cassandra')
            report = apport.Report()
            # ran into MemoryError loading retraced report with CoreDump
            with open('%s.new' % report_path, 'rb') as fp:
                report.load(fp)

            crash_signature = report.crash_signature()
            stacktrace_addr_sig = report.get('StacktraceAddressSignature', '')
            missing_dbgsym_pkg = False
            if 'RetraceOutdatedPackages' in report:
                if 'no debug symbol package' in \
                        report['RetraceOutdatedPackages']:
                    missing_dbgsym_pkg = True
            if not crash_signature:
                log('Apport did not return a crash_signature.')
                metrics.meter('retrace.missing.crash_signature')
                metrics.meter('retrace.missing.%s.crash_signature' %
                              architecture)
                metrics.meter('retrace.missing.%s.crash_signature' %
                              release)
                metrics.meter('retrace.missing.%s.%s.crash_signature' %
                              (release, architecture))
                if missing_dbgsym_pkg:
                    metrics.meter('retrace.missing.crash_signature. \
                                   no_dbgsym_pkg')
                    metrics.meter('retrace.missing.%s.crash_signature. \
                                   no_dbgsym_pkg' % release)
                log('StacktraceTop:')
                for line in report['StacktraceTop'].splitlines():
                    log(line)
                if architecture == 'armhf' and \
                        'RetraceOutdatedPackages' not in report:
                    log('Saved OOPS %s for manual investigation.' %
                        oops_id)
                    # create a new crash with the CoreDump for investigation
                    report['CoreDump'] = (core_file,)
                    failed_crash = '%s/%s.crash' % (failure_storage, oops_id)
                    with open(failed_crash, 'wb') as fp:
                        report.write(fp)
            original_sas = ''
            if stacktrace_addr_sig:
                if type(stacktrace_addr_sig) == unicode:
                    stacktrace_addr_sig = stacktrace_addr_sig.encode('utf-8')
                # if the OOPS doesn't already have a SAS add one
                try:
                    original_sas = self.oops_cf.get(oops_id, ['StacktraceAddressSignature'])['StacktraceAddressSignature']
                except NotFoundException:
                    self.oops_cf.insert(oops_id, {'StacktraceAddressSignature': stacktrace_addr_sig})
            else:
                metrics.meter('retrace.missing.stacktrace_address_signature')
                metrics.meter('retrace.missing.%s.stacktrace_address_signature' %
                              architecture)
                metrics.meter('retrace.missing.%s.stacktrace_address_signature' %
                              release)
                metrics.meter('retrace.missing.%s.%s.stacktrace_address_signature' %
                              (release, architecture))

            # Use the unretraced report's SAS for the index and stacktrace_cf,
            # otherwise use the one from the retraced report as apport / gdb
            # may improve
            if original_sas:
                stacktrace_addr_sig = original_sas

            crash_signature = utils.format_crash_signature(crash_signature)

            # only consider it a successful retrace if there is a Stacktrace
            # in the retraced report LP: #1321386
            if crash_signature and stacktrace_addr_sig and \
                    'Stacktrace' in report:
                if 'CoreDump' in report:
                    report.pop('CoreDump')
                try:
                    self.stacktrace_cf.insert(stacktrace_addr_sig, report)
                except MaximumRetryException:
                    total = sum(len(x) for x in report.values())
                    m = 'Could not fit data in a single insert (%s, %d):'
                    log(m % (path, total))
                    chunked_insert(self.stacktrace_cf, stacktrace_addr_sig, report)
                args = (release, day_key, retracing_time, True)
                self.update_retrace_stats(*args)
                log('Successfully retraced.')
                metrics.meter('retrace.success')
                metrics.meter('retrace.success.%s' % release)
                metrics.meter('retrace.success.%s' % architecture)
                metrics.meter('retrace.success.%s.%s' %
                              (release, architecture))
                # for having a total count of missing ddebs log outdated
                # packages for successful retraces too
                if 'RetraceOutdatedPackages' in report:
                    log('RetraceOutdatedPackages:')
                    for line in report['RetraceOutdatedPackages'].splitlines():
                        log('%s (%s)' % (line, release))
            else:
                # we aren't adding the report back to the stacktrace_cf so put
                # the CoreDump back in the report in case we save it
                report['CoreDump'] = (core_file,)
                if 'Stacktrace' not in report and crash_signature:
                    log('Stacktrace not in retraced report with a crash_sig.')
                    metrics.meter('retrace.missing.stacktrace')
                    metrics.meter('retrace.missing.%s.stacktrace' %
                                  architecture)
                    metrics.meter('retrace.missing.%s.stacktrace' %
                                  release)
                    metrics.meter('retrace.missing.%s.%s.stacktrace' %
                                  (release, architecture))
                    log('Saved OOPS %s for manual investigation.' %
                        oops_id)
                    # create a new crash with the CoreDump for investigation
                    failed_crash = '%s/%s.crash' % (failure_storage, oops_id)
                    with open(failed_crash, 'wb') as fp:
                        report.write(fp)

                # Given that we do not as yet keep debugging symbols around for
                # every package version ever released, it's worth knowing the
                # extent of the problem. If we ever decide to keep debugging
                # symbols for every package version, we can reprocess these
                # with a map/reduce job.
                log('Could not retrace.')

                if stacktrace_addr_sig:
                    crash_signature = 'failed:%s' % \
                        utils.format_crash_signature(stacktrace_addr_sig)
                else:
                    log('Retraced report missing stacktrace_addr_sig.')
                    metrics.meter('retrace.missing.stacktrace_addr_sig')
                    metrics.meter('retrace.missing.%s.stacktrace_addr_sig' %
                                  architecture)
                    metrics.meter('retrace.missing.%s.stacktrace_addr_sig' %
                                  release)
                    metrics.meter('retrace.missing.%s.%s.stacktrace_addr_sig' %
                                  (release, architecture))
                    log('Saved OOPS %s for manual investigation.' %
                        oops_id)
                    # create a new crash with the CoreDump for investigation
                    failed_crash = '%s/%s.crash' % (failure_storage, oops_id)
                    with open(failed_crash, 'wb') as fp:
                        report.write(fp)
                    self.move_to_failed_queue(msg)

                if 'Stacktrace' not in report:
                    failure_reason = 'No stacktrace after retracing'
                    log('Retraced report missing Stacktrace.')
                else:
                    failure_reason = 'No crash signature after retracing'

                if 'RetraceOutdatedPackages' in report:
                    # these counters will overlap with outdated_packages but
                    # that is okay
                    if missing_dbgsym_pkg:
                        metrics.meter('retrace.failure.missing_dbgsym')
                        metrics.meter('retrace.failure.%s.missing_dbgsym' % \
                                      release)
                    outdated_pkgs = []
                    missing_ddebs = []
                    log('RetraceOutdatedPackages:')
                    for line in report['RetraceOutdatedPackages'].splitlines():
                        if 'outdated debug symbol' in line:
                            outdated_pkgs.append(line.split(' ')[0])
                        if 'no debug symbol' in line:
                            missing_ddebs.append(line.split(' ')[-1])
                        log('%s (%s)' % (line, release))
                    if architecture == 'armhf' and missing_ddebs \
                            and not outdated_pkgs:
                        log('Saved OOPS %s for manual investigation.' %
                            oops_id)
                        # create a new crash with the CoreDump for investigation
                        failed_crash = '%s/%s.crash' % (failure_storage, oops_id)
                        with open(failed_crash, 'wb') as fp:
                            report.write(fp)
                    if not outdated_pkgs:
                        failure_reason += ' and missing ddebs.'
                    else:
                        failure_reason += ' and outdated packages.'
                    self.oops_cf.insert(oops_id,
                        {'RetraceFailureReason': failure_reason})
                    if outdated_pkgs:
                        outdated_pkg_count = len(outdated_pkgs)
                        outdated_pkgs.sort()
                        self.oops_cf.insert(oops_id,
                            {'RetraceFailureOutdatedPackages':
                             '%s' % ' '.join(outdated_pkgs)})
                    else:
                        outdated_pkgs = ''
                        outdated_pkg_count = 0
                    if missing_ddebs:
                        missing_ddeb_count = len(missing_ddebs)
                        missing_ddebs.sort()
                        self.oops_cf.insert(oops_id,
                            {'RetraceFailureMissingDebugSymbols':
                             '%s' % ' '.join(missing_ddebs)})
                    else:
                        missing_ddebs = ''
                        missing_ddeb_count = 0
                    if crash_signature:
                        try:
                            rf_reason = self.bucketretracefail_fam.get(crash_signature)
                            if 'missing_ddeb_count' in rf_reason:
                                least_missing_ddeb_count = \
                                    int(rf_reason['missing_ddeb_count'])
                            else:
                                least_missing_ddeb_count = 9999
                            if 'outdated_pkg_count' in rf_reason:
                                least_outdated_pkg_count = \
                                    int(rf_reason['outdated_pkg_count'])
                            else:
                                least_outdated_pkg_count = 9999
                        except NotFoundException:
                            least_missing_ddeb_count = 9999
                            least_outdated_pkg_count = 9999
                        if outdated_pkg_count < least_outdated_pkg_count and \
                                missing_ddeb_count < least_missing_ddeb_count:
                            self.bucketretracefail_fam.insert(crash_signature,
                                {'oops': oops_id,
                                 'missing_ddeb_count': '%s' % missing_ddeb_count,
                                 'outdated_pkg_count': '%s' % outdated_pkg_count,
                                 'Reason': failure_reason,
                                 'MissingDebugSymbols': '%s' % ' '.join(missing_ddebs),
                                 'OutdatedPackages': '%s' % ' '.join(outdated_pkgs)
                                })
                        metrics.meter('retrace.failure.outdated_packages')
                        metrics.meter('retrace.failure.%s.outdated_packages' % \
                                      release)
                        metrics.meter('retrace.failure.%s.outdated_packages' % \
                                      architecture)
                        metrics.meter('retrace.failure.%s.%s.outdated_packages' % \
                                      (release, architecture))
                    else:
                        pass
                else:
                    failure_reason += '.'
                    self.oops_cf.insert(oops_id,
                                        {'RetraceFailureReason':
                                         failure_reason})
                    if crash_signature:
                        self.bucketretracefail_fam.insert(
                            crash_signature,
                            {'oops': oops_id,
                             'Reason': failure_reason})
                args = (release, day_key, retracing_time, False)
                self.update_retrace_stats(*args)
                metrics.meter('retrace.failed')
                metrics.meter('retrace.failed.%s' % release)
                metrics.meter('retrace.failed.%s' % architecture)
                metrics.meter('retrace.failed.%s.%s' %
                              (release, architecture))

            # We want really quick lookups of whether we have a stacktrace for
            # this signature, so that we can quickly tell the client whether we
            # need a core dump from it.
            if stacktrace_addr_sig and crash_signature:
                self.indexes_fam.insert(
                    'crash_signature_for_stacktrace_address_signature',
                    {stacktrace_addr_sig : crash_signature})
            # Use the unretraced report's SAS for the index as these were
            # created with that version of the report
            if original_sas:
                self.indexes_fam.remove('retracing', [original_sas])
                # This will contain the OOPS ID we're currently processing as
                # well.
                gen = self.awaiting_retrace_fam.xget(original_sas)
                ids = [k for k,v in gen]
                oops_ids = ids
            else:
                # The initial report didn't have a SAS so don't check
                # awaiting_retrace
                ids = []

            if len(ids) == 0:
                # Handle eventual consistency. If the writes to AwaitingRetrace
                # haven't hit this node yet, that's okay. We'll clean up
                # unprocessed OOPS IDs from that CF at regular intervals later,
                # so just process this OOPS ID now.
                oops_ids = [oops_id]
                metrics.meter('missing.cannot_find_oopses_awaiting_retrace')

            if original_sas:
                try:
                    self.awaiting_retrace_fam.remove(original_sas, oops_ids)
                except NotFoundException:
                    # An oops may not exist in awaiting_retrace if the initial
                    # report didn't have a SAS
                    pass

            if crash_signature:
                self.bucket(oops_ids, crash_signature)
                if self.rebucket(crash_signature):
                    log('Recounting %s' % crash_signature)
                    self.recount(crash_signature, msg.channel)
        finally:
            rm_eff('%s' % report_path)
            rm_eff('%s.new' % report_path)
            rm_eff(core_file)

        log('Done processing %s' % path)
        self.processed(msg)

    def processed(self, msg):
        parts = msg.body.split(':', 1)
        oops_id = None
        oops_id, provider = parts
        removed = self.remove(*parts)
        if removed:
            # We've processed this. Delete it off the MQ.
            msg.channel.basic_ack(msg.delivery_tag)
            self.update_time_to_retrace(msg)
            return True
        return False

    def requeue(self, msg, oops_id):
        # RabbitMQ versions from 2.7.0 push basic_reject'ed messages
        # back onto the front of the queue:
        # http://www.rabbitmq.com/semantics.html
        # Build a new message from the old one, publish the new and bin
        # the old.
        ts = msg.properties.get('timestamp')
        # If we are still unable to find the OOPS after 8 days then
        # just process it as a failure.
        today = datetime.datetime.utcnow()
        target_date = today - datetime.timedelta(8)
        # if we don't know how old it is it must be ancient
        if not ts:
            log('Marked OOPS (%s) without timestamp as failed' % oops_id)
            # failed_to_process calls processed which removes the core
            self.failed_to_process(msg, oops_id, True)
            return
        if ts.date() < target_date.date():
            log('Marked old OOPS (%s) as failed' % oops_id)
            # failed_to_process calls processed which removes the core
            self.failed_to_process(msg, oops_id, True)
            return

        key = msg.delivery_info['routing_key']

        body = amqp.Message(msg.body, timestamp=ts)
        body.properties['delivery_mode'] = 2
        msg.channel.basic_publish(body, exchange='', routing_key=key)
        msg.channel.basic_reject(msg.delivery_tag, False)

    def update_time_to_retrace(self, msg):
        '''Record how long it took to retrace this crash, from the time we got
           a core file to the point that we got a either a successful or failed
           retrace out of it.
        '''
        timestamp = msg.properties.get('timestamp')
        if not timestamp:
            return

        time_taken = datetime.datetime.utcnow() - timestamp
        time_taken = time_taken.total_seconds()
        # This needs to be at a global level since it's dealing with the time
        # items have been sitting on a queue shared by all retracers.
        m = get_metrics('retracer.all')
        m.timing('timings.submission_to_retrace', time_taken)

    def rebucket(self, crash_signature):
        '''Rebucket any failed retraces into the bucket just created for the
        given correct crash signature.'''

        ids = []
        failed_key = 'failed:' + crash_signature
        ids = [k for k,v in self.bucket_fam.xget(failed_key)]

        if not ids:
            return False

        self.bucket(ids, crash_signature)

        # We don't have to remove the 'failed:' signature from
        # crash_signature_for_stacktrace_address_signature as we'll simply
        # overwrite it with the correct crash signature.
        return True

    def recount(self, crash_signature, channel):
        '''Put on another queue to correct all the day counts.'''

        channel.queue_declare(queue='recount', durable=True, auto_delete=False)
        body = amqp.Message(crash_signature)
        body.properties['delivery_mode'] = 2
        channel.basic_publish(body, exchange='', routing_key='recount')

    def bucket(self, ids, crash_signature):
        '''Insert the provided set of OOPS ids into the bucket with the given
        crash signature.'''

        for oops_id in ids:
            try:
                o = self.oops_cf.get(oops_id)
            except NotFoundException:
                log('Could not find %s for %s.' % (oops_id, crash_signature))
                o = {}
            utils.bucket(self.oops_config, oops_id, crash_signature, o)
            metrics.meter('success.binary_bucketed')
            if not crash_signature.startswith('failed:') and o:
                self.cleanup_oops(oops_id)

    def cleanup_oops(self, oops_id):
        '''Remove no longer needed columns from the OOPS column family for a
           specific OOPS id.'''
        unneeded_columns = ['Disassembly', 'ProcMaps', 'ProcStatus',
                            'Registers', 'StacktraceTop']
        self.oops_cf.remove(oops_id, columns=unneeded_columns)

def parse_options():
    parser = argparse.ArgumentParser(description='Process core dumps.')
    parser.add_argument('--config-dir',
                        help='Packaging system config base directory.',
                        required=True)
    parser.add_argument('-a', '--architecture',
                        help='architecture to process (e. g. i386 or armhf)',
                        required=True)
    parser.add_argument('--sandbox-dir',
                        help='Directory for state information. Subdirectories '
                        'will be created for each release for which crashes '
                        'have been seen, with subdirectories under those for '
                        'each instance of this program. Future runs will '
                        'assume that any already downloaded package is also '
                        'extracted to this sandbox.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Print extra information during each retrace.')
    parser.add_argument('--failed', action='store_true',
                        help='Only process previously failed retraces.')
    parser.add_argument('--nocache-debs', action='store_true',
                        help='Do not cache downloaded debs.')
    parser.add_argument('--nouse-sandbox', action='store_true',
                        help='Do not use the sandbox directory.')
    parser.add_argument('--cleanup-sandbox', action='store_true',
                        default=False,
                        help='wipe the sandbox directory after a retrace.')
    parser.add_argument('--cleanup-debs', action='store_true',
                        default=False,
                        help='wipe the deb cache directory after a retrace.')
    parser.add_argument('-o', '--output', help='Log messages to a file.')
    parser.add_argument('--retrieve-core',
                        help=('Debug processing a single uuid:provider_id.'
                              'This does not touch Cassandra or the queue.'))
    return parser.parse_args()

def main():
    global log_output
    global root_handler
    # should move to a configuration option
    global failure_storage
    failure_storage = '/srv/daisy.ubuntu.com/production/var'

    options = parse_options()
    if options.output:
        path = '%s' % (options.output)
        sys.stdout.close()
        sys.stdout = open(path, 'a')
        sys.stderr.close()
        sys.stderr = sys.stdout

    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)

    retracer_oops_cfg = oops_dictconfig.config_from_dict(config.oops_config)
    retracer_oops_cfg.template['reporter'] = 'retracer'

    try:
        msg = "Running"
        if 'revno' in version_info:
            revno = version_info['revno']
            msg += " revision number: %s" % revno
            record_revno()
        if options.sandbox_dir:
            msg += " with sandbox_dir %s" % options.sandbox_dir
        if options.nocache_debs:
            msg += " and not caching debs."
        else:
            msg += "."
        log(msg)

        retracer = Retracer(options.config_dir, options.sandbox_dir,
                            options.architecture, options.verbose,
                            not options.nocache_debs, not options.nouse_sandbox,
                            options.cleanup_sandbox, options.cleanup_debs,
                            failed=options.failed)
        if options.retrieve_core:
            parts = options.one_off.split(':', 1)
            path, oops_id = retracer.write_bucket_to_disk(parts[0], parts[1])
            log('Wrote %s to %s. Exiting.' % (path, oops_id))
        else:
            retracer.listen()
    except:
        context = dict(exc_info=sys.exc_info())
        report = retracer_oops_cfg.create(context)
        ids = retracer_oops_cfg.publish(report)
        # log ids if you want
        raise

if __name__ == '__main__':
    main()

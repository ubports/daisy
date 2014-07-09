#!/usr/bin/python
# -*- coding: utf-8 -*-
# 
# Copyright © 2011-2013 Canonical Ltd.
# Author: Evan Dandrea <evan.dandrea@canonical.com>
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
import atexit
import os
import sys
import tempfile
import shutil
from subprocess import Popen, PIPE
import apport
from pycassa import ConsistencyLevel
from pycassa.columnfamily import ColumnFamily
from pycassa.cassandra.ttypes import NotFoundException
from pycassa.pool import MaximumRetryException
from pycassa.types import IntegerType, FloatType, UTF8Type
import argparse
import time
import socket
import re
from daisy.metrics import wrapped_connection_pool, get_metrics, revno
from daisy import utils
from daisy.version import version_info
import logging
from daisy import config
from oopsrepository import config as oopsconfig
import traceback
import datetime

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

class Retracer:
    def __init__(self, config_dir, sandbox_dir, architecture, verbose,
                 cache_debs, failed=False):
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
        self.oops_fam = ColumnFamily(self.pool, 'OOPS')
        self.indexes_fam = ColumnFamily(self.pool, 'Indexes')
        self.stack_fam = ColumnFamily(self.pool, 'Stacktrace')
        self.awaiting_retrace_fam = ColumnFamily(self.pool, 'AwaitingRetrace')
        # Retry counter increments. This may result in double counting, but
        # we'd end up risking that anyway if failing with a timeout exception
        # and then re-rerunning the retrace later.
        self.retrace_stats_fam = ColumnFamily(self.pool, 'RetraceStats',
                                              retry_counter_mutations=True)
        self.bucket_fam = ColumnFamily(self.pool, 'Bucket')

        # We didn't set a default_validation_class for these in the schema.
        # Whoops.
        self.oops_fam.default_validation_class = UTF8Type()
        self.indexes_fam.default_validation_class = UTF8Type()
        self.stack_fam.default_validation_class = UTF8Type()
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

    def failed_to_process(self, msg, oops_id):
        self.processed(msg)
        # Also remove it from the retracing index, if we haven't already.
        try:
            addr_sig = self.oops_fam.get(oops_id,
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
            log('swift token: %s' % str( _cached_swift.token))
            bucket = provider_data['bucket']
            _cached_swift.delete_object(bucket, key)
        except swiftclient.client.ClientException:
            log('Could not remove %s (swift):' % key)
            log(traceback.format_exc())
            metrics.meter('swift_delete_error')

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
            self.remove_from_swift(oops_id, provider_data)
        elif t == 's3':
            self.remove_from_s3(oops_id, provider_data)
        elif t == 'local':
            self.remove_from_local(oops_id, provider_data)

    @prefix_log_with_amqp_message
    def callback(self, msg):
        log('Processing.')
        parts = msg.body.split(':', 1)
        oops_id, provider = parts
        try:
            quorum = ConsistencyLevel.QUORUM
            col = self.oops_fam.get(oops_id, read_consistency_level=quorum)
        except NotFoundException:
            # We do not have enough information at this point to be able to
            # remove this from the retracing row in the Indexes CF. Throw it
            # back on the queue and hope that eventual consistency works its
            # magic by then.
            log('Unable to find in OOPS CF.')
            # RabbitMQ versions from 2.7.0 push basic_reject'ed messages
            # back onto the front of the queue:
            # http://www.rabbitmq.com/semantics.html
            # Build a new message from the old one, publish the new and bin
            # the old.
            ts = msg.properties.get('timestamp')
            # 2014-06-12 Set missing old OOPSes (in newcassandra) as failed
            if ts < datetime.datetime(2014, 6, 11):
                log('Marked old OOPS (%s) as failed' % oops_id)
                # failed_to_process calls processed which removes the core
                self.failed_to_process(msg, oops_id)
                return

            key = msg.delivery_info['routing_key']

            body = amqp.Message(msg.body, timestamp=ts)
            body.properties['delivery_mode'] = 2
            msg.channel.basic_publish(body, exchange='', routing_key=key)
            msg.channel.basic_reject(msg.delivery_tag, False)

            metrics.meter('could_not_find_oops')
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

        try:
            if p2.returncode != 0:
                log('Error processing %s:' % path)
                if ret[1]:
                    for line in ret[1].splitlines():
                        log(line)
                # We couldn't decompress this, so there's no value in trying again.
                self.processed(msg)
                return

            report = apport.Report()

            for k in col:
                report[k.encode('UTF-8')] = col[k].encode('UTF-8')

            release = report.get('DistroRelease', '')
            bad = '[^-a-zA-Z0-9_.() ]+'
            retraceable = utils.retraceable_release(release)
            invalid = re.search(bad, release) or len(release) > 1024
            if not release or invalid or not retraceable:
                self.processed(msg)
                return

            report['CoreDump'] = (core_file,)
            report_path = '%s.crash' % path
            with open(report_path, 'wb') as fp:
                report.write(fp)
        finally:
            rm_eff(core_file)

        try:
            log('Retracing {}'.format(msg.body))
            sandbox, cache = self.setup_cache(self.sandbox_dir, release)
            day_key = time.strftime('%Y%m%d', time.gmtime())

            retracing_start_time = time.time()
            cmd = ['python3', self.apport_retrace_path, report_path, '-c',
                   '-S', self.config_dir, '--sandbox-dir', sandbox,
                   '-o', '%s.new' % report_path]
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
            raise
        finally:
            rm_eff(report_path)

        try:
            if proc.returncode != 0:
                if proc.returncode == 99:
                    # Transient apt error, like "failed to fetch ... size
                    # mismatch"
                    log('Transient apport error.')
                    # Throw back onto the queue
                    msg.channel.basic_reject(msg.delivery_tag, True)
                    return
                # apport-retrace will exit 0 even on a failed retrace unless
                # something has gone wrong at a lower level, as was the case
                # when python-apt bailed out on invalid sources.list files.
                # Fail hard so we do not incorrectly write a lot of retraces to
                # the database as failures.
                m = 'Retrace failed (%i), moving to failed queue:'
                log(m % proc.returncode)
                for std in (out, err):
                    for line in std.splitlines():
                        log(line)
                self.move_to_failed_queue(msg)
                retracing_time = time.time() - retracing_start_time
                self.update_retrace_stats(release, day_key, retracing_time,
                                          crashed=True)
                return

            retracing_time = time.time() - retracing_start_time

            if not os.path.exists('%s.new' % report_path):
                log('%s.new did not exist.' % report_path)
                self.failed_to_process(msg, oops_id)
                return

            log('Writing back to Cassandra')
            report = apport.Report()
            with open('%s.new' % report_path, 'rb') as fp:
                report.load(fp)

            crash_signature = report.crash_signature()
            stacktrace_addr_sig = report.get('StacktraceAddressSignature', '')
            if not crash_signature:
                log('Apport did not return a crash_signature.')
                log('StacktraceTop:')
                for line in report['StacktraceTop'].splitlines():
                    log(line)
            original_sas = ''
            if stacktrace_addr_sig:
                if type(stacktrace_addr_sig) == unicode:
                    stacktrace_addr_sig = stacktrace_addr_sig.encode('utf-8')
                # if the OOPS doesn't already have a SAS add one
                try:
                    original_sas = self.oops_fam.get(oops_id, ['StacktraceAddressSignature'])['StacktraceAddressSignature']
                except NotFoundException:
                    self.oops_fam.insert(oops_id, {'StacktraceAddressSignature': stacktrace_addr_sig})

            crash_signature = utils.format_crash_signature(crash_signature)
            # if there are any outdated packages don't write to the
            # Stacktrace column family LP: #1321386
            if crash_signature and 'RetraceOutdatedPackages' not in report:
                try:
                    self.stack_fam.insert(stacktrace_addr_sig, report)
                except MaximumRetryException:
                    total = sum(len(x) for x in report.values())
                    m = 'Could not fit data in a single insert (%s, %d):'
                    log(m % (path, total))
                    chunked_insert(self.stack_fam, stacktrace_addr_sig, report)
                args = (release, day_key, retracing_time, True)
                self.update_retrace_stats(*args)
                log('Successfully retraced.')
            else:
                # Given that we do not as yet keep debugging symbols around for
                # every package version ever released, it's worth knowing the
                # extent of the problem. If we ever decide to keep debugging
                # symbols for every package version, we can reprocess these
                # with a map/reduce job.

                if stacktrace_addr_sig:
                    crash_signature = 'failed:%s' % \
                        utils.format_crash_signature(stacktrace_addr_sig)
                else:
                    crash_signature = ''

                log('Could not retrace.')
                if 'RetraceOutdatedPackages' in report:
                    log('RetraceOutdatedPackages:')
                    for line in report['RetraceOutdatedPackages'].splitlines():
                        log(line)
                args = (release, day_key, retracing_time, False)
                self.update_retrace_stats(*args)

            # Use the unretraced report's SAS for the index, otherwise use the
            # one from the retraced report as apport / gdb may improve
            if original_sas:
                stacktrace_addr_sig = original_sas
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
            rm_eff('%s.new' % report_path)

        log('Done processing %s' % path)
        self.processed(msg)

    def processed(self, msg):
        parts = msg.body.split(':', 1)
        oops_id = None
        oops_id, provider = parts
        self.remove(*parts)

        # We've processed this. Delete it off the MQ.
        msg.channel.basic_ack(msg.delivery_tag)
        self.update_time_to_retrace(msg)

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
                o = self.oops_fam.get(oops_id)
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
        self.oops_fam.remove(oops_id, columns=unneeded_columns)

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
    parser.add_argument('-o', '--output', help='Log messages to a file.')
    parser.add_argument('--retrieve-core',
                        help=('Debug processing a single uuid:provider_id.'
                              'This does not touch Cassandra or the queue.'))
    return parser.parse_args()

def main():
    global log_output
    global root_handler

    options = parse_options()
    if options.output:
        path = '%s.%s' % (options.output, options.architecture)
        if 'revno' in version_info:
            path = '%s.%s' % (path, version_info['revno'])
        sys.stdout.close()
        sys.stdout = open(path, 'a')
        sys.stderr.close()
        sys.stderr = sys.stdout

    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)

    retracer = Retracer(options.config_dir, options.sandbox_dir,
                        options.architecture, options.verbose,
                        not options.nocache_debs, failed=options.failed)
    if options.retrieve_core:
        parts = options.one_off.split(':', 1)
        path, oops_id = retracer.write_bucket_to_disk(parts[0], parts[1])
        log('Wrote %s to %s. Exiting.' % (path, oops_id))
    else:
        revno('retracer')
        retracer.listen()

if __name__ == '__main__':
    main()

#!/usr/bin/python
# -*- coding: utf-8 -*-
# 
# Copyright © 2011-2012 Canonical Ltd.
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
import utils
import re
import metrics
import logging

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass
if not configuration:
    import configuration

from oopsrepository import config

def log(message, level=logging.INFO):
    logging.log(level, message)

def chunked_insert(cf, row_key, data):
    # The thrift_framed_transport_size_in_mb limit is 15 MB by default, but
    # there seems to be some additional overhead between 64 and 128 bytes.
    # max_size = (1024 * 1024 * 15 - 128)
    # Production doesn't seem to like 15 MB, so lets play it conservatively.
    max_size = (1024 * 1024 * 8)

    for key in data:
        val = data[key].encode('utf-8')
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
        assert which.returncode == 0, 'Cannot find apport-retrace in $PATH (%s)' % os.environ.get('PATH')

    def setup_cassandra(self):
        os.environ['OOPS_KEYSPACE'] = configuration.cassandra_keyspace
        self.oops_config = config.get_config()
        self.oops_config['host'] = configuration.cassandra_hosts
        self.oops_config['username'] = configuration.cassandra_username
        self.oops_config['password'] = configuration.cassandra_password

        pool = metrics.failure_wrapped_connection_pool()
        self.oops_fam = ColumnFamily(pool, 'OOPS')
        self.indexes_fam = ColumnFamily(pool, 'Indexes')
        self.stack_fam = ColumnFamily(pool, 'Stacktrace')
        self.awaiting_retrace_fam = ColumnFamily(pool, 'AwaitingRetrace')
        self.retrace_stats_fam = ColumnFamily(pool, 'RetraceStats')
        self.bucket_fam = ColumnFamily(pool, 'Buckets')

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
            if configuration.amqp_username and configuration.amqp_password:
                connection = amqp.Connection(host=configuration.amqp_host,
                                        userid=configuration.amqp_username,
                                        password=configuration.amqp_password)
            else:
                connection = amqp.Connection(host=configuration.amqp_host)
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
                        # Don't probe immediately, give the network/process
                        # time to come back.
                        time.sleep(0.1)
                    else:
                        raise
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
        if crashed:
            status = ':crashed'
        elif not success:
            status = ':failed'
        else:
            status = ':success'
        # We can't mix counters and other data types
        self.retrace_stats_fam.add(day_key, release + status)

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
        # Remove it from the retracing queue.
        msg.channel.basic_ack(msg.delivery_tag)

        # Add it to the failed to retrace queue.
        queue = 'failed_retrace_%s' % self.architecture
        msg.channel.queue_declare(queue=queue, durable=True, auto_delete=False)
        body = amqp.Message(msg.body)
        # Persistent
        body.properties['delivery_mode'] = 2
        msg.channel.basic_publish(body, exchange='', routing_key=queue)

    def failed_to_process(self, msg, oops_id):
        msg.channel.basic_ack(msg.delivery_tag)
        # Also remove it from the retracing index, if we haven't already.
        try:
            addr_sig = self.oops_fam.get(oops_id,
                            ['StacktraceAddressSignature'],
                            read_consistency_level=ConsistencyLevel.QUORUM)
            addr_sig = addr_sig.values()[0]
            self.indexes_fam.remove('retracing', [addr_sig])
        except NotFoundException:
            pass

    def write_s3_bucket_to_disk(self, bucket_id):
        from boto.s3.connection import S3Connection, OrdinaryCallingFormat
        from boto.exception import S3ResponseError

        conn = S3Connection(aws_access_key_id=configuration.aws_access_key,
                            aws_secret_access_key=configuration.aws_secret_key,
                            port=3333,
                            host=configuration.ec2_host,
                            is_secure=False,
                            calling_format=OrdinaryCallingFormat())
        try:
            bucket = conn.get_bucket(configuration.ec2_bucket)
            key = bucket.get_key(bucket_id)
        except S3ResponseError as e:
            self.failed_to_process(msg, msg.body)
            return None
        fp = tempfile.mkstemp()
        with open(fp[1], 'wb') as fp:
            for data in key:
                # 8K at a time.
                fp.write(data)
            path = fp.name
        return path

    def callback(self, msg):
        log('Processing %s' % msg.body)
        if not msg.body.startswith('/'):
            oops_id = msg.body
            path = self.write_s3_bucket_to_disk(oops_id)
        else:
            path = msg.body
            oops_id = msg.body.rsplit('/', 1)[1]

        if not os.path.exists(path):
            self.failed_to_process(msg, oops_id)
            return

        new_path = '%s.core' % path
        with open(new_path, 'wb') as fp:
            log('Decompressing to %s' % new_path)
            p1 = Popen(['base64', '-d', path], stdout=PIPE)
            p2 = Popen(['zcat'], stdin=p1.stdout, stdout=fp)
            ret = p2.communicate()
        if p2.returncode != 0:
            log('Error processing %s:\n%s' % (path, ret[1]))
            # We've processed this. Delete it off the MQ.
            msg.channel.basic_ack(msg.delivery_tag)
            try:
                os.remove(path)
            except OSError, e:
                if e.errno != 2:
                    raise
            try:
                os.remove(new_path)
            except OSError, e:
                if e.errno != 2:
                    raise
            return

        report = apport.Report()
        # TODO use oops-repository instead
        col = self.oops_fam.get(oops_id, read_consistency_level=ConsistencyLevel.QUORUM)
        for k in col:
            report[k.encode('UTF-8')] = col[k].encode('UTF-8')
        
        release = report.get('DistroRelease', '')
        bad = '[^-a-zA-Z0-9_.() ]+'
        retraceable = utils.retraceable_release(release)
        invalid = re.search(bad, release) or len(release) > 1024
        if not release or invalid or not retraceable:
            msg.channel.basic_ack(msg.delivery_tag)
            for p in (path, new_path):
                try:
                    os.remove(p)
                except OSError, e:
                    if e.errno != 2:
                        raise
            return

        report['CoreDump'] = (new_path,)
        report_path = '%s.crash' % path
        with open(report_path, 'wb') as fp:
            report.write(fp)

        log('Retracing')
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
        proc = Popen(cmd, env=env, stderr=PIPE, universal_newlines=True)
        err = proc.communicate()[1]
        if proc.returncode != 0:
            if proc.returncode == 99:
                # Transient apt error, like "failed to fetch ... size mismatch"
                # Throw back onto the queue by not ack'ing it.
                return
            # apport-retrace will exit 0 even on a failed retrace unless
            # something has gone wrong at a lower level, as was the case when
            # python-apt bailed out on invalid sources.list files. Fail hard so
            # we do not incorrectly write a lot of retraces to the database as
            # failures.
            log('Retrace failed (%i), moving to failed queue:\n%s' % (proc.returncode, err))
            self.move_to_failed_queue(msg)
            retracing_time = time.time() - retracing_start_time
            self.update_retrace_stats(release, day_key, retracing_time,
                                      crashed=True)
            return

        retracing_time = time.time() - retracing_start_time

        has_signature = False
        if os.path.exists('%s.new' % report_path):
            log('Writing back to Cassandra')
            report = apport.Report()
            with open('%s.new' % report_path, 'rb') as fp:
                report.load(fp)
            stacktrace_addr_sig = report['StacktraceAddressSignature']

            crash_signature = report.crash_signature()
            if crash_signature:
                has_signature = True
                crash_signature = crash_signature.encode('utf-8')

        if has_signature:
            try:
                self.stack_fam.insert(stacktrace_addr_sig, report)
            except MaximumRetryException:
                total = sum(len(x) for x in report.values())
                log('Could not fit data in a single insert (%s, %d):' % (path, total))
                chunked_insert(self.stack_fam, stacktrace_addr_sig, report)
            self.update_retrace_stats(release, day_key, retracing_time, True)
        else:
            # Given that we do not as yet keep debugging symbols around for
            # every package version ever released, it's worth knowing the
            # extent of the problem. If we ever decide to keep debugging
            # symbols for every package version, we can reprocess these with a
            # map/reduce job.

            stacktrace_addr_sig = report['StacktraceAddressSignature']
            crash_signature = 'failed:%s' % stacktrace_addr_sig
            crash_signature = crash_signature.encode('utf-8')
            log('Could not retrace.')
            # FIXME UnicodeDecodeError: 'ascii' codec can't decode byte 0xe2 in
            # position 66: ordinal not in range(128)
            #if 'Stacktrace' in report:
            #    log('Stacktrace:')
            #    log(report['Stacktrace'])
            #else:
            #    log('No stacktrace.')
            if 'RetraceOutdatedPackages' in report:
                log('RetraceOutdatedPackages:')
                log(report['RetraceOutdatedPackages'])
            self.update_retrace_stats(release, day_key, retracing_time, False)

        # We want really quick lookups of whether we have a stacktrace for this
        # signature, so that we can quickly tell the client whether we need a
        # core dump from it.
        self.indexes_fam.insert(
            'crash_signature_for_stacktrace_address_signature',
            {stacktrace_addr_sig : crash_signature})

        self.indexes_fam.remove('retracing', [stacktrace_addr_sig])

        oops_ids = [oops_id]
        try:
            # This will contain the OOPS ID we're currently processing as well.
            ids = self.awaiting_retrace_fam.get(stacktrace_addr_sig)
            oops_ids += ids.keys()
        except NotFoundException:
            # Handle eventual consistency. If the writes to AwaitingRetrace
            # haven't hit this node yet, that's okay. We'll clean up
            # unprocessed OOPS IDs from that CF at regular intervals later, so
            # just process this OOPS ID now.
            pass

        self.bucket(oops_ids, crash_signature)

        try:
            self.awaiting_retrace_fam.remove(stacktrace_addr_sig, oops_ids)
        except NotFoundException:
            # I'm not sure why this would happen, but we could safely continue
            # on were it to.
            pass

        if has_signature:
            if self.rebucket(crash_signature):
                self.recount(crash_signature, msg.channel)

        for p in (path, new_path, report_path, '%s.new' % report_path):
            try:
                os.remove(p)
            except OSError, e:
                if e.errno != 2:
                    raise
        log('Done processing %s' % path)
        # We've processed this. Delete it off the MQ.
        msg.channel.basic_ack(msg.delivery_tag)

    def rebucket(self, crash_signature):
        '''Rebucket any failed retraces into the bucket just created for the
        given correct crash signature.'''

        ids = []
        try:
            # TODO might need to use xget here if the bucket is large.
            ids = self.bucket_fam.get('failed:' + crash_signature)
            ids = ids.keys()
        except NotFoundException:
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
                vals = self.oops_fam.get(oops_id, ['DistroRelease', 'Package'],
                                read_consistency_level=ConsistencyLevel.QUORUM)
            except NotFoundException:
                vals = {}
            utils.bucket(self.oops_config, oops_id, crash_signature, vals)

def parse_options():
    parser = argparse.ArgumentParser(description='Process core dumps.')
    parser.add_argument('--config-dir',
                        help='Packaging system configuration base directory.',
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
    return parser.parse_args()

def main():
    global log_output
    options = parse_options()
    fmt = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
    if options.output:
        sys.stdout.close()
        sys.stdout = open(options.output, 'a')
        sys.stderr.close()
        sys.stderr = sys.stdout
    fmt = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
    logging.basicConfig(format=fmt, level=logging.INFO)

    retracer = Retracer(options.config_dir, options.sandbox_dir,
                        options.architecture, options.verbose,
                        not options.nocache_debs, failed=options.failed)
    retracer.listen()

if __name__ == '__main__':
    main()

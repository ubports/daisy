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
from pycassa.columnfamily import ColumnFamily
from pycassa.cassandra.ttypes import NotFoundException
from pycassa.types import IntegerType, FloatType
import argparse
import time
import socket
import utils
import re
import metrics

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass
if not configuration:
    import configuration

from oopsrepository import config

def get_architecture():
    try:
        p = Popen(['dpkg-architecture', '-qDEB_HOST_ARCH'], stdout=PIPE)
        return p.communicate()[0].strip('\n')
    except OSError, e:
        print >>sys.stderr, 'Could not determine architecture: %s' % str(e)
        sys.exit(1)

class Retracer:
    def __init__(self, config_dir, sandbox_dir, verbose):
        self.setup_cassandra()
        self.config_dir = config_dir
        self.sandbox_dir = sandbox_dir
        self.verbose = verbose
        self.architecture = get_architecture()
        # A mapping of release names to temporary sandbox and cache
        # directories, so that we can remove them at the end of the run.
        # TODO: we should create a single temporary directory that all of these
        # live under, saving the multiple calls to atexit.register.
        self._sandboxes = {}
        # The time we were last able to talk to the AMQP server.
        self._lost_connection = None

    def setup_cassandra(self):
        os.environ['OOPS_KEYSPACE'] = configuration.cassandra_keyspace
        self.oops_config = config.get_config()
        self.oops_config['host'] = [configuration.cassandra_host]

        pool = metrics.failure_wrapped_connection_pool()
        self.oops_fam = ColumnFamily(pool, 'OOPS')
        self.indexes_fam = ColumnFamily(pool, 'Indexes')
        self.stack_fam = ColumnFamily(pool, 'Stacktrace')
        self.awaiting_retrace_fam = ColumnFamily(pool, 'AwaitingRetrace')
        self.retrace_stats_fam = ColumnFamily(pool, 'RetraceStats')

    def listen(self):
        retrace = 'retrace_%s' % self.architecture
        connection = None
        channel = None
        try:
            connection = amqp.Connection(host=configuration.amqp_host)
            channel = connection.channel()
            channel.queue_declare(queue=retrace, durable=True,
                                  auto_delete=False)
            channel.basic_qos(0, 1, False)
            print 'Waiting for messages. ^C to exit.'
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
        channel.basic_cancel(tag)

    def update_retrace_stats(self, release, day_key, retracing_time,
                             success=True):
        """
        release: the distribution release, ex. 'Ubuntu 12.04'
        day_key: the date as a YYYYMMDD string.
        retracing_time: the amount of time it took to retrace.
        success: whether crash was retraceable.
        """
        status = ':success'
        if not success:
            status = ':failed'
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
        cache = os.path.join(instance_sandbox, 'cache')
        os.mkdir(sandbox)
        os.mkdir(cache)
        self._sandboxes[release] = (sandbox, cache)
        return self._sandboxes[release]

    def callback(self, msg):
        print 'Processing', msg.body
        path = msg.body
        try:
            oops_id = msg.body.rsplit('/', 1)[1]
        except IndexError:
            # If we accidentally put something other than a full path on the
            # queue.
            print 'Could not parse message:', path
            msg.channel.basic_ack(msg.delivery_tag)
            return
        if not os.path.exists(path):
            print path, 'does not exist, skipping.'
            # We've processed this. Delete it off the MQ.
            msg.channel.basic_ack(msg.delivery_tag)
            # Also remove it from the retracing index, if we haven't already.
            try:
                addr_sig = self.oops_fam.get(oops_id,
                                ['StacktraceAddressSignature'])
                addr_sig = addr_sig.values()[0]
                self.indexes_fam.remove('retracing', [addr_sig])
            except NotFoundException:
                pass
            return

        new_path = '%s.core' % path
        with open(new_path, 'wb') as fp:
            print 'Decompressing to', new_path
            p1 = Popen(['base64', '-d', path], stdout=PIPE)
            p2 = Popen(['zcat'], stdin=p1.stdout, stdout=fp)
            ret = p2.communicate()
        if p2.returncode != 0:
            print >>sys.stderr, 'Error processing %s:\n%s' % (path, ret[1])
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
        col = self.oops_fam.get(oops_id)
        for k in col:
            report[k.encode('UTF-8')] = col[k].encode('UTF-8')
        
        release = report.get('DistroRelease', '')
        bad = '[^-a-zA-Z0-9_.() ]+'
        if not release or re.search(bad, release) or len(release) > 1024:
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
        with open(report_path, 'w') as fp:
            report.write(fp)

        print 'Retracing'
        sandbox, cache = self.setup_cache(self.sandbox_dir, release)
        day_key = time.strftime('%Y%m%d', time.gmtime())

        retracing_start_time = time.time()
        cmd = ['apport-retrace', report_path, '-c', '-S', self.config_dir,
               '-C', cache, '--sandbox-dir', sandbox,
               '-o', '%s.new' % report_path]
        if self.verbose:
            cmd.append('-v')
        proc = Popen(cmd)
        proc.communicate()
        retracing_time = time.time() - retracing_start_time

        if proc.returncode == 0 and os.path.exists('%s.new' % report_path):
            print 'Writing back to Cassandra'
            report = apport.Report()
            with open('%s.new' % report_path, 'r') as fp:
                report.load(fp)
            stacktrace_addr_sig = report['StacktraceAddressSignature']

            crash_signature = report.crash_signature()
            if crash_signature:
                self.stack_fam.insert(stacktrace_addr_sig, report)
                self.update_retrace_stats(release, day_key, retracing_time,
                                          True)
            else:
                crash_signature = 'failed:%s' % stacktrace_addr_sig
                print 'Could not retrace.'
                self.update_retrace_stats(release, day_key, retracing_time,
                                          False)
        else:
            # Given that we do not as yet keep debugging symbols around for
            # every package version ever released, it's worth knowing the
            # extent of the problem. If we ever decide to keep debugging
            # symbols for every package version, we can reprocess these with a
            # map/reduce job.
            stacktrace_addr_sig = report['StacktraceAddressSignature']
            crash_signature = 'failed:%s' % stacktrace_addr_sig
            print 'Could not retrace: apport-retrace returned', proc.returncode
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
            oops_ids = self.awaiting_retrace_fam.get(stacktrace_addr_sig)
            oops_ids = oops_ids.keys()
        except NotFoundException:
            # Handle eventual consistency. If the writes to AwaitingRetrace
            # haven't hit this node yet, that's okay. We'll clean up
            # unprocessed OOPS IDs from that CF at regular intervals later, so
            # just process this OOPS ID now.
            pass
        for oops_id in oops_ids:
            try:
                vals = self.oops_fam.get(oops_id, ['DistroRelease', 'Package'])
            except NotFoundException:
                vals = {}
            utils.bucket(self.oops_config, oops_id, crash_signature, vals)

        try:
            self.awaiting_retrace_fam.remove(stacktrace_addr_sig, oops_ids)
        except NotFoundException:
            # I'm not sure why this would happen, but we could safely continue
            # on were it to.
            pass
        for p in (path, new_path, report_path, '%s.new' % report_path):
            try:
                os.remove(p)
            except OSError, e:
                if e.errno != 2:
                    raise
        print 'Done processing', path
        # We've processed this. Delete it off the MQ.
        msg.channel.basic_ack(msg.delivery_tag)

def parse_options():
    parser = argparse.ArgumentParser(description='Process core dumps.')
    parser.add_argument('--config-dir',
                        help='Packaging system configuration base directory.',
                        required=True)
    parser.add_argument('--sandbox-dir',
                        help='Directory for state information. Subdirectories '
                        'will be created for each release for which crashes '
                        'have been seen, with subdirectories under those for '
                        'each instance of this program. Future runs will '
                        'assume that any already downloaded package is also '
                        'extracted to this sandbox.')
    parser.add_argument('-v', '--verbose',
                        help='Print extra information during each retrace.')
    return parser.parse_args()

def main():
    options = parse_options()
    retracer = Retracer(options.config_dir, options.sandbox_dir,
                        options.verbose)
    retracer.listen()

if __name__ == '__main__':
    main()

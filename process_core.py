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
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.cassandra.ttypes import NotFoundException
from pycassa.types import IntegerType, FloatType
import argparse
import time
import socket

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass
if not configuration:
    import configuration

from oopsrepository import config, oopses

os.environ['OOPS_KEYSPACE'] = configuration.cassandra_keyspace
oops_config = config.get_config()
oops_config['host'] = [configuration.cassandra_host]

oops_fam = None
indexes_fam = None
stack_fam = None
awaiting_retrace_fam = None
retrace_stats_fam = None

config_dir = None
sandbox_dir = None
_sandboxes = {}

def callback(msg):
    print 'Processing', msg.body
    path = msg.body
    oops_id = msg.body.rsplit('/', 1)[1]
    if not os.path.exists(path):
        print path, 'does not exist, skipping.'
        # We've processed this. Delete it off the MQ.
        msg.channel.basic_ack(msg.delivery_tag)
        # Also remove it from the retracing index, if we haven't already.
        try:
            addr_sig = oops_fam.get(oops_id, ['StacktraceAddressSignature'])
            addr_sig = addr_sig.values()[0]
            indexes_fam.remove('retracing', [addr_sig])
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
    col = oops_fam.get(oops_id)
    for k in col:
        report[k.encode('UTF-8')] = col[k].encode('UTF-8')
    
    report['CoreDump'] = (new_path,)
    report_path = '%s.crash' % path
    with open(report_path, 'w') as fp:
        report.write(fp)
    print 'Retracing'
    release = report['DistroRelease']
    sandbox, cache = setup_cache(sandbox_dir, release)
    day_key = time.strftime('%Y%m%d', time.gmtime())
    retracing_start_time = time.time()
    proc = Popen(['apport-retrace', report_path, '-c', '-S', config_dir,
                  '-C', cache, '--sandbox-dir', sandbox,
                  '-o', '%s.new' % report_path])
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
            stack_fam.insert(stacktrace_addr_sig, report)
            update_retrace_stats(release, day_key, retracing_time, True)
        else:
            crash_signature = 'failed:%s' % stacktrace_addr_sig
            print 'Could not retrace.'
            update_retrace_stats(release, day_key, retracing_time, False)
    else:
        # Given that we do not as yet keep debugging symbols around for every
        # package version ever released, it's worth knowing the extent of the
        # problem. If we ever decide to keep debugging symbols for every
        # package version, we can reprocess these with a map/reduce job.
        stacktrace_addr_sig = report['StacktraceAddressSignature']
        crash_signature = 'failed:%s' % stacktrace_addr_sig
        print 'Could not retrace.'
        update_retrace_stats(release, day_key, retracing_time, False)

    # We want really quick lookups of whether we have a stacktrace for
    # this signature, so that we can quickly tell the client whether we
    # need a core dump from it.
    indexes_fam.insert(
        'crash_signature_for_stacktrace_address_signature',
        {stacktrace_addr_sig : crash_signature})

    indexes_fam.remove('retracing', [stacktrace_addr_sig])

    oops_ids = [oops_id]
    try:
        # This will contain the OOPS ID we're currently processing as well.
        oops_ids = awaiting_retrace_fam.get(stacktrace_addr_sig)
        oops_ids = oops_ids.keys()
    except NotFoundException:
        # Handle eventual consistency. If the writes to AwaitingRetrace haven't
        # hit this node yet, that's okay. We'll clean up unprocessed OOPS IDs
        # from that CF at regular intervals later, so just process this OOPS ID
        # now.
        pass
    for oops_id in oops_ids:
        oopses.bucket(oops_config, oops_id, crash_signature)

    try:
        awaiting_retrace_fam.remove(stacktrace_addr_sig, oops_ids)
    except NotFoundException:
        # I'm not sure why this would happen, but we could safely continue on
        # were it to.
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

def update_retrace_stats(release, day_key, retracing_time, success=True):
    status = ':success'
    if not success:
        status = ':failed'
    # We can't mix counters and other data types
    retrace_stats_fam.add(day_key, release + status)

    # Compute the cumulative moving average
    mean_key = '%s:%s' % (day_key, release)
    count_key = '%s:count' % mean_key
    indexes_fam.column_validators = {mean_key: FloatType(),
                                    count_key: IntegerType()}
    try:
        mean = indexes_fam.get('mean_retracing_time', column_start=mean_key,
                               column_finish=count_key)
    except NotFoundException:
        mean = {mean_key: 0.0, count_key: 0}

    new_mean = float((retracing_time + mean[count_key] * mean[mean_key]) /
                      mean[count_key] + 1)
    mean[mean_key] = new_mean
    mean[count_key] += 1
    indexes_fam.insert('mean_retracing_time', mean)

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
    return parser.parse_args()

def get_architecture():
    try:
        p = Popen(['dpkg-architecture', '-qDEB_HOST_ARCH'], stdout=PIPE)
        return p.communicate()[0].strip('\n')
    except OSError, e:
        print >>sys.stderr, 'Could not determine architecture: %s' % str(e)
        sys.exit(1)

def setup_cassandra():
    global oops_fam, indexes_fam, stack_fam, awaiting_retrace_fam
    global retrace_stats_fam
    pool = ConnectionPool(configuration.cassandra_keyspace,
                          [configuration.cassandra_host])
    oops_fam = ColumnFamily(pool, 'OOPS')
    indexes_fam = ColumnFamily(pool, 'Indexes')
    stack_fam = ColumnFamily(pool, 'Stacktrace')
    awaiting_retrace_fam = ColumnFamily(pool, 'AwaitingRetrace')
    retrace_stats_fam = ColumnFamily(pool, 'RetraceStats')

def setup_cache(sandbox_dir, release):
    if release in _sandboxes:
        return _sandboxes[release]
    sandbox_release = os.path.join(sandbox_dir, release)
    if not os.path.exists(sandbox_release):
        os.makedirs(sandbox_release)
    instance_sandbox = tempfile.mkdtemp(dir=sandbox_release)
    atexit.register(shutil.rmtree, instance_sandbox)
    # Write a pid file so that if we have to wipe out a cache that has grown
    # too large we can stop the retracer responsible for it before doing so.
    with open(os.path.join(instance_sandbox, 'pid'), 'w') as fp:
        fp.write('%d' % os.getpid())
    sandbox = os.path.join(instance_sandbox, 'sandbox')
    cache = os.path.join(instance_sandbox, 'cache')
    os.mkdir(sandbox)
    os.mkdir(cache)
    _sandboxes[release] = (sandbox, cache)
    return _sandboxes[release]

_lost_connection = None
def run_forever(channel, callback, queue):
    global _lost_connection
    tag = channel.basic_consume(callback=callback, queue=queue)
    try:
        while (not _lost_connection or time.time() < _lost_connection + 120):
            try:
                channel.wait()
            except (socket.error, IOError), e:
                is_amqplib_ioerror = (type(e) is IOError and
                                      e.args == ('Socket error',))
                amqplib_conn_errors = (socket.error,
                                       amqp.AMQPConnectionException)
                is_amqplib_conn_error = isinstance(e, amqplib_conn_errors)
                if is_amqplib_conn_error or is_amqplib_ioerror:
                    _lost_connection = time.time()
                    # Don't probe immediately, give the network/process time to
                    # come back.
                    time.sleep(0.1)
                else:
                    raise
    except KeyboardInterrupt:
        pass
    channel.basic_cancel(tag)

def main():
    global config_dir, sandbox_dir
    options = parse_options()
    config_dir = options.config_dir
    sandbox_dir = options.sandbox_dir

    arch = get_architecture()
    setup_cassandra()

    connection = amqp.Connection(host=configuration.amqp_host)
    channel = connection.channel()
    atexit.register(connection.close)
    atexit.register(channel.close)

    retrace = 'retrace_%s' % arch
    channel.queue_declare(queue=retrace, durable=True, auto_delete=False)

    channel.basic_qos(0, 1, False)
    print 'Waiting for messages. ^C to exit.'
    run_forever(channel, callback, queue='retrace_%s' % arch)

if __name__ == '__main__':
    main()

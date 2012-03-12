#!/usr/bin/python
# -*- coding: utf-8 -*-
# 
# Copyright © 2011 Canonical Ltd.
# Author: Evan Dandrea <evan.dandrea@canonical.com>
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import amqplib.client_0_8 as amqp
import atexit
import os
import sys
from subprocess import Popen, PIPE
import apport
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.cassandra.ttypes import NotFoundException
import argparse
import configuration

oops_fam = None
indexes_fam = None
stack_fam = None
awaiting_retrace_fam = None
bucket_fam = None

cache_dir = None
config_dir = None

def callback(msg):
    print 'Processing', msg.body
    path = msg.body
    oops_id = msg.body.rsplit('/', 1)[1]
    if not os.path.exists(path):
        print path, 'does not exist, skipping.'
        # We've processed this. Delete it off the MQ.
        msg.channel.basic_ack(msg.delivery_tag)
        os.remove(path)
        # Also remove it from the retracing index, if we haven't already.
        try:
            addr_sig = oops_fam.get(oops_id, ['StacktraceAddressSignature'])
            addr_sig = addr_sig.values()[0]
            indexes_fam.remove('retracing', [addr_sig])
        except NotFoundException:
            pass

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
        os.remove(path)
        os.remove(new_path)
        return

    report = apport.Report()
    # TODO use oops-repository instead
    col = oops_fam.get(oops_id)
    for k in col:
        report[k] = col[k]
    
    report['CoreDump'] = (new_path,)
    report_path = '%s.crash' % path
    with open(report_path, 'w') as fp:
        report.write(fp)
    print 'Retracing'
    proc = Popen(['apport-retrace', report_path, '-S', config_dir, '-C',
                  cache_dir, '-o', '%s.new' % report_path])
    proc.communicate()
    if proc.returncode == 0 and os.path.exists('%s.new' % report_path):
        print 'Writing back to Cassandra'
        report = apport.Report()
        report.load(open('%s.new' % report_path, 'r'))
        # Unnecessary to hold onto this, plus it causes pycassa to time out on
        # insert.
        del report['CoreDump']
        crash_signature = report.crash_signature()
        if signature:
            stacktrace_addr_sig = report['StacktraceAddressSignature']
            stack_fam.insert(stacktrace_addr_sig, report)
        else:
            # I do not expect to ever see these.
            crash_signature = 'failed'
    else:
        # Given that we do not as yet keep debugging symbols around for every
        # package version ever released, it's worth knowing the extent of the
        # problem. If we ever decide to keep debugging symbols for every
        # package version, we can reprocess these with a map/reduce job.
        stacktrace_addr_sig = report['StacktraceAddressSignature']
        crash_signature = 'failed:%s' % stacktrace_addr_sig
        print 'Could not retrace.'

    # We want really quick lookups of whether we have a stacktrace for
    # this signature, so that we can quickly tell the client whether we
    # need a core dump from it.
    indexes_fam.insert(
        'crash_signature_for_stacktrace_address_signature',
        {stacktrace_addr_sig : crash_signature})

    indexes_fam.remove('retracing', [stacktrace_addr_sig])

    oops_ids = [oops_id]
    try:
        oops_ids = awaiting_retrace_fam.get(stacktrace_addr_sig)
        oops_ids = oops_ids.keys()
    except NotFoundException:
        # Handle eventual consistency. If the writes to AwaitingRetrace haven't
        # hit this node yet, that's okay. We'll clean up unprocessed OOPS IDs
        # from that CF at regular intervals later, so just process this OOPS ID
        # now.
        pass
    for oops_id in oops_ids:
        bucket_fam.insert(crash_signature, {oops_id : ''}

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

def parse_options():
    parser = argparse.ArgumentParser(description='Process core dumps.')
    parser.add_argument('--config-dir',
                        help='Packaging system configuration base directory.',
                        required=True)
    parser.add_argument('--cache',
                        help='Cache directory for packages downloaded in the '
                             'sandbox.')
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
    pool = ConnectionPool(configuration.cassandra_keyspace,
                          [configuration.cassandra_host])
    oops_fam = ColumnFamily(pool, 'OOPS')
    indexes_fam = ColumnFamily(pool, 'Indexes')
    stack_fam = ColumnFamily(pool, 'Stacktrace')
    awaiting_retrace_fam = ColumnFamily(pool, 'AwaitingRetrace')
    bucket_fam = ColumnFamily(pool, 'Buckets')

def main():
    global cache_dir, config_dir
    options = parse_options()
    cache_dir = options.cache
    config_dir = options.config_dir

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
    tag = channel.basic_consume(callback=callback, queue='retrace_%s' % arch)
    try:
        while True:
            channel.wait()
    except KeyboardInterrupt:
        pass
    channel.basic_cancel(tag)

if __name__ == '__main__':
    main()

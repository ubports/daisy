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

import pika
import atexit
import os
import sys
from subprocess import Popen, PIPE
import apport
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from hashlib import md5
import argparse
import configuration

oops_fam = None
indexes_fam = None
stack_fam = None
cache_dir = None
config_dir = None

def callback(ch, method, props, path):
    print 'Processing', path
    if not os.path.exists(path):
        print path, 'does not exist, skipping.'
        # We've processed this. Delete it off the MQ.
        ch.basic_ack(delivery_tag=method.delivery_tag)
        os.remove(path)

    new_path = '%s.core' % path
    with open(new_path, 'wb') as fp:
        print 'Decompressing to', new_path
        p1 = Popen(['base64', '-d', path], stdout=PIPE)
        p2 = Popen(['zcat'], stdin=p1.stdout, stdout=fp)
        ret = p2.communicate()
    if p2.returncode != 0:
        print >>sys.stderr, 'Error processing %s:\n%s' % (path, ret[1])
        # We've processed this. Delete it off the MQ.
        ch.basic_ack(delivery_tag=method.delivery_tag)
        os.remove(path)
        os.remove(new_path)
        return

    report = apport.Report()
    uuid = path.rsplit('/', 1)[1]
    # TODO use oops-repository instead
    col = oops_fam.get(uuid)
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
    # TODO Put failed traces on a failed queue.
    if proc.returncode == 0:
        print 'Writing back to Cassandra'
        report = apport.Report()
        report.load(open('%s.new' % report_path, 'r'))
        stacktrace_addr_sig = report['StacktraceAddressSignature']
        stacktrace = report['Stacktrace']
        hashed_stack = md5(stacktrace).hexdigest()

        # We want really quick lookups of whether we have a stacktrace
        # for this signature, so that we can quickly tell the client
        # whether we need a core dump from it.
        indexes_fam.insert('stacktrace_hashes_by_signature',
            {stacktrace_addr_sig : hashed_stack})
        stack_fam.insert(hashed_stack, {'stacktrace' : stacktrace})
    else:
        print 'Could not retrace.'

    # We've processed this. Delete it off the MQ.
    ch.basic_ack(delivery_tag=method.delivery_tag)
    for p in (path, new_path, report_path, '%s.new' % report_path):
        try:
            os.remove(p)
        except OSError, e:
            if e.errno != 2:
                raise
    print 'Done processing', path

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
    global oops_fam, indexes_fam, stack_fam
    pool = ConnectionPool(configuration.cassandra_keyspace,
                          [configuration.cassandra_host])
    oops_fam = ColumnFamily(pool, 'OOPS')
    indexes_fam = ColumnFamily(pool, 'Indexes')
    stack_fam = ColumnFamily(pool, 'Stacktrace')

def main():
    global cache_dir, config_dir
    options = parse_options()
    cache_dir = options.cache
    config_dir = options.config_dir

    arch = get_architecture()
    setup_cassandra()

    params = pika.ConnectionParameters(host=configuration.amqp_host)
    connection = pika.BlockingConnection(params)
    atexit.register(connection.close)
    channel = connection.channel()

    for queue in ('retrace_amd64', 'retrace_i386'):
        channel.queue_declare(queue=queue, durable=True)

    channel.basic_qos(prefetch_count=1)
    print 'Waiting for messages. ^C to exit.'
    channel.basic_consume(callback, queue='retrace_%s' % arch)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()

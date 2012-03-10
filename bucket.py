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
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.cassandra.ttypes import NotFoundException
import configuration
import atexit
import apport

oops_fam = None
indexes_fam = None
bucket_fam = None

def callback(msg):
    oops_id = msg.body
    oops = None
    try:
        oops = oops_fam.get(oops_id)
    except NotFoundException:
        # Handle eventual consistency. This could occur if we have a really
        # quick bucketing, like a Python crash that goes straight here. Leave
        # it on the queue to be processed later.
        # 
        # NOTE that if we ever want to let the user delete crashes, it will
        # break here.
        return
    report = apport.Report()
    for key in oops:
        report[key] = oops[key]
    if ('InterpreterPath' not in report and
        'StacktraceAddressSignature' in report):
        try:
            crash_signature = indexes_fam.get(
                'crash_signature_for_stacktrace_address_signature',
                [report['StacktraceAddressSignature']])
        except NotFoundException:
            # Handle eventual consistency. If we pulled this report straight
            # off the bucket queue, the write to the Indexes CF may not have
            # finished propagating. Leave the OOPS ID on the queue to be
            # processed later.
            return
    else:
        crash_signature = report.crash_signature()

    if not crash_signature:
        crash_signature = 'failed'

    # In the future we'll have server-side bug patterns. These are rules for a
    # particular package or specific bucket that either help refine the bucket
    # or provide the developer with needed additional information.
    bucket_fam.insert(crash_signature, {oops_id, ''})
    msg.channel.basic_ack(msg.delivery_tag)

def setup_cassandra():
    global oops_fam, indexes_fam, bucket_fam
    pool = ConnectionPool(configuration.cassandra_keyspace,
                          [configuration.cassandra_host])
    oops_fam = ColumnFamily(pool, 'OOPS')
    indexes_fam = ColumnFamily(pool, 'Indexes')
    bucket_fam = ColumnFamily(pool, 'Buckets')

def main():
    setup_cassandra()

    connection = amqp.Connection(host=configuration.amqp_host)
    channel = connection.channel()
    atexit.register(connection.close)
    atexit.register(channel.close)

    channel.queue_declare(queue='bucket', durable=True, auto_delete=False)
    channel.basic_qos(0, 1, False)
    print 'Waiting for messages. ^C to exit.'
    tag = channel.basic_consume(callback=callback, queue='bucket')
    try:
        while True:
            channel.wait()
    except KeyboardInterrupt:
        pass
    channel.basic_cancel(tag)

if __name__ == '__main__':
    main()

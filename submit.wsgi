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

import uuid
import bson

import os
from oopsrepository import config, oopses
import pycassa
from pycassa.cassandra.ttypes import NotFoundException
import configuration
import amqplib.client_0_8 as amqp
import atexit
import apport

os.environ['OOPS_KEYSPACE'] = configuration.cassandra_keyspace
oops_config = config.get_config()
oops_config['host'] = [configuration.cassandra_host]

# Cassandra connections. These may move into oopsrepository in the future.
pool = pycassa.ConnectionPool(configuration.cassandra_keyspace,
                          [configuration.cassandra_host])
indexes_fam = pycassa.ColumnFamily(pool, 'Indexes')
awaiting_retrace_fam = pycassa.ColumnFamily(pool, 'AwaitingRetrace')
bucket_fam = pycassa.ColumnFamily(pool, 'Buckets')

# Rabbit MQ bucket queue.
connection = amqp.Connection(host=configuration.amqp_host)
channel = connection.channel()
atexit.register(connection.close)
atexit.register(channel.close)
channel.queue_declare(queue='bucket', durable=True, auto_delete=False)

content_type = 'CONTENT_TYPE'
ostream = 'application/octet-stream'
response_headers = [('Content-type', 'text/plain')]

def ok_response(start_response, data=''):
    if data:
        start_response('200 OK', response_headers)
    else:
        start_response('200 OK', [])
    return [data]

def bad_request_response(start_response):
    start_response('400 Bad Request', [])
    return ['']

def application(environ, start_response):
    global oops_config
    global pool, indexes_fam, awaiting_retrace_fam, bucket_fam
    global channel

    if not environ.has_key(content_type) and environ[content_type] == ostream:
        return ok_response(start_response)

    data = environ['wsgi.input'].read()
    user_token = None
    # / + 128 character system UUID
    if len(environ['PATH_INFO']) == 129:
        user_token = environ['PATH_INFO'][1:]
    oops_id = str(uuid.uuid1())
    try:
        # TODO: Remove the insert_bson method from oopsrepository, replacing it
        # with insert_dict.
        oopses.insert_bson(oops_config, oops_id, data, user_token)
    except bson.errors.InvalidBSON:
        return bad_request_response(start_response)

    data = bson.BSON(data).decode()

    if 'InterpreterPath' in data and not 'StacktraceAddressSignature' in data:
        # Python crashes can be immediately bucketed.
        report = apport.Report()
        try:
            for key in ('ExecutablePath', 'Traceback', 'ProblemType'):
                report[key] = data[key]
        crash_signature = report.crash_signature()
        bucket_fam.insert(crash_signature, {oops_id : ''})
        return ok_response(start_response)

    addr_sig = data.get('StacktraceAddressSignature', None)
    if not addr_sig:
        # We received BSON data with unexpected keys.
        return bad_request_response(start_response)

    # Binary
    output = ''
    crash_sig = None
    try:
        crash_sig = indexes_fam.get(
            'crash_signature_for_stacktrace_address_signature', [addr_sig])
    except NotFoundException:
        pass
    if crash_sig:
        # We have already retraced for this address signature, so this crash
        # can be immediately bucketed.
        body = amqp.Message(oops_id)
        body.properties['delivery_mode'] = 2
        channel.basic_publish(body, exchange='', routing_key='bucket')
    else:
        # Are we already waiting for this stacktrace address signature to be
        # retraced?
        waiting = True
        try:
            indexes_fam.get('retracing', [addr_sig])
        except NotFoundException:
            waiting = False

        if not waiting:
            # We do not have a core file in the queue, so ask for one. Do
            # not assume we're going to get one, so also add this ID the
            # the AwaitingRetrace CF queue as well.
            output = '%s CORE' % oops_id

        awaiting_retrace_fam.insert(addr_sig, {oops_id : ''})
            
    return ok_response(start_response, output)

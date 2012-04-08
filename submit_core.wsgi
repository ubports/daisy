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

from cgi import parse_qs, escape
import amqplib.client_0_8 as amqp
import pycassa
from pycassa.cassandra.ttypes import NotFoundException
import shutil
import atexit

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass
if not configuration:
    import configuration
import os

ostream = 'application/octet-stream'
connection = amqp.Connection(host=configuration.amqp_host)
channel = connection.channel()
atexit.register(connection.close)
atexit.register(channel.close)

# Cassandra connections. These may move into oopsrepository in the future.
pool = pycassa.ConnectionPool(configuration.cassandra_keyspace,
                              [configuration.cassandra_host])
indexes_fam = pycassa.ColumnFamily(pool, 'Indexes')
oops_fam = pycassa.ColumnFamily(pool, 'OOPS')

def application(environ, start_response):
    global channel
    params = parse_qs(environ.get('QUERY_STRING', ''))
    uuid = ''
    if params and 'uuid' in params and 'arch' in params:
        uuid = escape(params['uuid'][0])
        arch = escape(params['arch'][0])
        if environ.has_key('CONTENT_TYPE') and environ['CONTENT_TYPE'] == ostream:
            path = os.path.join(configuration.san_path, uuid)
            queue = 'retrace_%s' % arch
            addr_sig = None
            try:
                addr_sig = oops_fam.get(uuid, ['StacktraceAddressSignature'])
                addr_sig = addr_sig.values()[0]
            except NotFoundException:
                # Due to Cassandra's eventual consistency model, we may receive
                # the core dump before the OOPS has been written to all the
                # nodes. This is acceptable, as we'll just ask the next user
                # for a core dump.
                pass
            if not addr_sig:
                start_response('400 Bad Request', [])
                return ['']
            copied = False
            with open(path, 'w') as fp:
                try:
                    shutil.copyfileobj(environ['wsgi.input'], fp, 512)
                    copied = True
                except IOError, e:
                    if e.message != 'request data read error':
                        raise
            if not copied:
                try:
                    os.remove(path)
                except OSError:
                    pass
                start_response('400 Bad Request', [])
                return ['']

            os.chmod(path, 0o666)
            channel.queue_declare(queue=queue, durable=True, auto_delete=False)
            body = amqp.Message(path)
            # Persistent
            body.properties['delivery_mode'] = 2
            channel.basic_publish(body, exchange='', routing_key=queue)
            indexes_fam.insert('retracing', {addr_sig : ''})
        
    start_response('200 OK', [])
    return [uuid]

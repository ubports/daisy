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

from cgi import parse_qs, escape
import pika
import shutil
import atexit
import configuration

ostream = 'application/octet-stream'
params = pika.ConnectionParameters(host=configuration.amqp_host)
connection = pika.BlockingConnection(params)
channel = connection.channel()
atexit.register(connection.close)

def application(environ, start_response):
    params = parse_qs(environ.get('QUERY_STRING'))
    uuid = ''
    if params and 'uuid' in params and 'arch' in params:
        uuid = escape(params['uuid'][0])
        arch = escape(params['arch'][0])
        if environ.has_key('CONTENT_TYPE') and environ['CONTENT_TYPE'] == ostream:
            path = os.path.join(configuration.san_path, uuid)
            queue = 'retrace_%s' % arch
            with open(path, 'w') as fp:
                shutil.copyfileobj(environ['wsgi.input'], fp, 512)
                channel.queue_declare(queue=queue, durable=True)
                channel.basic_publish(
                    exchange='', routing_key=queue, body=path,
                    properties=pika.BasicProperties(delivery_mode=2))
        
    start_response('200 OK', [])
    return [uuid]

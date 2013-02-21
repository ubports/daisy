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

from cgi import parse_qs
import amqplib.client_0_8 as amqp
import pycassa
from pycassa.cassandra.ttypes import NotFoundException
import shutil
import atexit
import utils
import re
import tempfile

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass
if not configuration:
    from daisy import configuration
import os
import metrics

ostream = 'application/octet-stream'
if configuration.amqp_username and configuration.amqp_password:
    connection = amqp.Connection(host=configuration.amqp_host,
                                 userid=configuration.amqp_username,
                                 password=configuration.amqp_password)
else:
    connection = amqp.Connection(host=configuration.amqp_host)
channel = connection.channel()
atexit.register(connection.close)
atexit.register(channel.close)

# Cassandra connections. These may move into oopsrepository in the future.
pool = metrics.failure_wrapped_connection_pool()
indexes_fam = pycassa.ColumnFamily(pool, 'Indexes')
oops_fam = pycassa.ColumnFamily(pool, 'OOPS')

path_filter = re.compile('[^a-zA-Z0-9-_]')

def write_to_s3(fileobj, oops_id):
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
    except S3ResponseError as e:
        bucket = conn.create_bucket(configuration.ec2_bucket)

    with tempfile.NamedTemporaryFile(mode='w+b') as fp:
        try:
            shutil.copyfileobj(fileobj, fp, 512)
        except IOError as e:
            if e.message != 'request data read error':
                raise
            return False

        fp.seek(0)
        key = bucket.new_key(oops_id)
        try:
            key.set_contents_from_file(fp)
        except IOError, e:
            if e.message == 'request data read error':
                return False
            else:
                raise

    return True

def write_to_san(fileobj, oops_id):
    '''Write the core file to SAN/NFS.'''

    path = os.path.join(configuration.san_path, oops_id)
    with open(path, 'w') as fp:
        try:
            shutil.copyfileobj(fileobj, fp, 512)
            copied = True
        except IOError, e:
            if e.message != 'request data read error':
                raise

    if copied:
        os.chmod(path, 0o666)
        return True
    else:
        try:
            os.remove(path)
        except OSError:
            pass
        return False

def wsgi_handler(environ, start_response):
    global channel
    params = parse_qs(environ.get('QUERY_STRING', ''))
    uuid = ''
    if params and 'uuid' in params and 'arch' in params:
        uuid = path_filter.sub('', params['uuid'][0])
        arch = path_filter.sub('', params['arch'][0])
        if environ.has_key('CONTENT_TYPE') and environ['CONTENT_TYPE'] == ostream:
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

            written = False
            if not configuration.ec2_bucket:
                written = write_to_san(environ['wsgi.input'], uuid)
                message = os.path.join(configuration.san_path, uuid)
            else:
                written = write_to_s3(environ['wsgi.input'], uuid)
                message = uuid

            if not written:
                start_response('400 Bad Request', [])
                return ['']

            channel.queue_declare(queue=queue, durable=True, auto_delete=False)
            body = amqp.Message(message)
            # Persistent
            body.properties['delivery_mode'] = 2
            channel.basic_publish(body, exchange='', routing_key=queue)
            indexes_fam.insert('retracing', {addr_sig : ''})
        
    start_response('200 OK', [])
    return [uuid]

application = utils.wrap_in_oops_wsgi(wsgi_handler,
                                      configuration.oops_repository,
                                      'daisy.ubuntu.com')

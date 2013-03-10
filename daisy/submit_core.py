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
import pycassa
from pycassa.cassandra.ttypes import NotFoundException
import shutil
import os
import random
from daisy import config

def swift_delete_ignoring_error(conn, bucket, oops_id):
    import swiftclient
    try:
        conn.delete_object(bucket, oops_id)
    except swiftclient.ClientException:
        pass

def write_to_swift(fileobj, oops_id, provider_data):
    '''Write the core file to OpenStack Swift.'''
    import swiftclient
    opts = {'tenant_name': provider_data['os_tenant_name'],
            'region_name': provider_data['os_region_name']}
    conn = swiftclient.client.Connection(provider_data['os_auth_url'],
                                         provider_data['os_username'],
                                         provider_data['os_password'],
                                         os_options=opts,
                                         auth_version='2.0')
    bucket = provider_data['bucket']
    conn.put_container(bucket)
    try:
        # Don't set a content_length (that we don't have) to force a chunked
        # transfer.
        conn.put_object(bucket, oops_id, fileobj)
    except IOError, e:
        swift_delete_ignoring_error(conn, bucket, oops_id)
        if e.message == 'request data read error':
            return False
        else:
            raise
    except swiftclient.ClientException:
        swift_delete_ignoring_error(conn, bucket, oops_id)
        return False
    return True

def s3_delete_ignoring_error(bucket, oops_id):
    from boto.exception import S3ResponseError
    try:
        bucket.delete_key(oops_id)
    except S3ResponseError:
        pass

def write_to_s3(fileobj, oops_id, provider_data):
    '''Write the core file to Amazon S3.'''
    from boto.s3.connection import S3Connection
    from boto.exception import S3ResponseError

    conn = S3Connection(aws_access_key_id=provider_data['aws_access_key'],
                        aws_secret_access_key=provider_data['aws_secret_key'],
                        host=provider_data['host'])
    try:
        bucket = conn.get_bucket(provider_data['bucket'])
    except S3ResponseError:
        bucket = conn.create_bucket(provider_data['bucket'])

    key = bucket.new_key(oops_id)
    try:
        key.set_contents_from_stream(fileobj)
    except IOError, e:
        s3_delete_ignoring_error(bucket, oops_id)
        if e.message == 'request data read error':
            return False
        else:
            raise
    except S3ResponseError:
        s3_delete_ignoring_error(bucket, oops_id)
        return False

    return True

def write_to_local(fileobj, oops_id, provider_data):
    '''Write the core file to local.'''

    path = os.path.join(provider_data['path'], oops_id)
    copied = False
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

def write_to_storage_provider(fileobj, uuid):
    # We trade deteriminism for forgetfulness and make up for it by passing the
    # decision along with the UUID as part of the queue message.
    r = random.randint(1, 100)
    provider = None
    for key, ranges in config.write_weight_ranges.iteritems():
        if r >= (ranges[0]*100) and r <= (ranges[1]*100):
            provider = key
            provider_data = config.core_storage[key]
            break

    written = False
    message = uuid
    t = provider_data['type']
    if t == 'swift':
        written = write_to_swift(fileobj, uuid, provider_data)
    elif t == 's3':
        written = write_to_s3(fileobj, uuid, provider_data)
    elif t == 'local':
        written = write_to_local(fileobj, uuid, provider_data)

    message = '%s:%s' % (message, provider)

    if written:
        return message
    else:
        return None

def submit(_pool, fileobj, uuid, arch):
    indexes_fam = pycassa.ColumnFamily(_pool, 'Indexes')
    oops_fam = pycassa.ColumnFamily(_pool, 'OOPS')

    try:
        addr_sig = oops_fam.get(uuid, ['StacktraceAddressSignature'])
        addr_sig = addr_sig.values()[0]
    except NotFoundException:
        # Due to Cassandra's eventual consistency model, we may receive
        # the core dump before the OOPS has been written to all the
        # nodes. This is acceptable, as we'll just ask the next user
        # for a core dump.
        msg = 'No matching address signature for this core dump.'
        return (False, msg)


    message = write_to_storage_provider(fileobj, uuid)
    if not message:
        return (False, '')

    if config.amqp_username and config.amqp_password:
        connection = amqp.Connection(host=config.amqp_host,
                                     userid=config.amqp_username,
                                     password=config.amqp_password)
    else:
        connection = amqp.Connection(host=config.amqp_host)
    channel = connection.channel()

    try:
        queue = 'retrace_%s' % arch
        channel.queue_declare(queue=queue, durable=True, auto_delete=False)
        body = amqp.Message(message)
        # Persistent
        body.properties['delivery_mode'] = 2
        channel.basic_publish(body, exchange='', routing_key=queue)
    finally:
        channel.close()
        connection.close()

    indexes_fam.insert('retracing', {addr_sig : ''})

    return (True, uuid)

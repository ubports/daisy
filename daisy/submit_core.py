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
import tempfile
import os

config = None
try:
    import local_config as config
except ImportError:
    pass
if not config:
    from daisy import configuration as config


def write_to_swift(fileobj, oops_id):
    import swiftclient
    opts = {'tenant_name': config.os_tenant_name, 
            'region_name': config.os_region_name}
    conn = swiftclient.client.Connection(config.os_auth_url,
                                         config.os_username,
                                         config.os_password,
                                         os_options=opts,
                                         auth_version='2.0')
    conn.put_container(config.swift_bucket)
    # Don't set a content_length (that we don't have) to force a chunked
    # transfer.
    conn.put_object(config.swift_bucket, oops_id, fileobj)
    return True

def write_to_s3(fileobj, oops_id):
    from boto.s3.connection import S3Connection
    from boto.exception import S3ResponseError

    conn = S3Connection(aws_access_key_id=config.aws_access_key,
                        aws_secret_access_key=config.aws_secret_key,
                        host=config.ec2_host)
    try:
        bucket = conn.get_bucket(config.ec2_bucket)
    except S3ResponseError as e:
        bucket = conn.create_bucket(config.ec2_bucket)

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

    path = os.path.join(config.san_path, oops_id)
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


    written = False
    if config.swift_bucket:
        written = write_to_swift(fileobj, uuid)
        message = uuid
    elif config.ec2_bucket:
        written = write_to_s3(fileobj, uuid)
        message = uuid
    else:
        written = write_to_san(fileobj, uuid)
        message = os.path.join(config.san_path, uuid)

    if not written:
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

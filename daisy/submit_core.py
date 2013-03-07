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

config = None
try:
    import local_config as config
except ImportError:
    pass
if not config:
    from daisy import configuration as config


def validate_and_set_configuration():
    write_weights = getattr(config, 'storage_write_weights', '') 
    core_storage = getattr(config, 'core_storage', '')
    if core_storage and not write_weights:
        msg = 'storage_write_weights must be set alongside core_storge.'
        raise ImportError(msg)
    if not core_storage:
        swift = getattr(config, 'swift_bucket', '')
        ec2 = getattr(config, 'ec2_bucket', '')
        local = getattr(config, 'san_path', '')
        if ec2 and swift:
            raise ImportError('ec2_bucket and swift_bucket cannot both be set.')

        # Match the old behaviour. Put everything on swift, if available.
        # Failing that, fall back to EC2, then local.
        if swift:
            os_auth_url = getattr(config, 'os_auth_url', '')
            os_username = getattr(config, 'os_username', '')
            os_password = getattr(config, 'os_password', '')
            os_tenant_name = getattr(config, 'os_tenant_name', '')
            os_region_name = getattr(config, 'os_region_name', '')
            config.storage_write_weights = { 'swift' : 1.0 }
            config.core_storage = {
                'default' : 'swift',
                  'swift' : {'type': 'swift',
                             'bucket': swift,
                             'os_auth_url': os_auth_url,
                             'os_username': os_username,
                             'os_password': os_password,
                             'os_tenant_name': os_tenant_name,
                             'os_region_name': os_region_name}, }
        elif ec2:
            host = getattr(config, 'ec2_host', '')
            aws_access_key = getattr(config, 'aws_access_key', '')
            aws_secret_key = getattr(config, 'aws_secret_key', '')
            if not (host and aws_access_key and aws_secret_key):
                msg = ('EC2 provider set but host, bucket, aws_access_key, or'
                       ' aws_secret_key not set.')
                raise ImportError(msg)
            config.storage_write_weights = { 's3' : 1.0 }
            config.core_storage = {
                'default' : 's3',
                    's3' : {'type': 's3', 'host': host, 'bucket': ec2,
                             'aws_access_key': aws_access_key,
                             'aws_secret_key': aws_secret_key}, }
        elif local:
            config.storage_write_weights = { 'local' : 1.0 }
            config.core_storage = {
                'default' : 'local',
                    'local' : {'type': 'local', 'path': local}, }
        else:
            raise ImportError('no core storage provider is set.')


    if not getattr(config, 'storage_write_weights', ''):
        d = config.core_storage.get('default', '')
        if not d:
            msg = ('No storage_write_weights set, but no default set in core'
                   ' storage')
            raise ImportError(msg)
        config.storage_write_weights = { d : 1.0 }

    for k, v in config.core_storage.iteritems():
        if k == 'default':
            continue
        t = v.get('type', '')
        if not t:
            raise ImportError('You must set a type for %s.' % k)
        if t == 'swift':
            keys = ['bucket', 'os_auth_url', 'os_username', 'os_password',
                    'os_tenant_name', 'os_region_name']
        elif t == 's3':
            keys = ['host', 'bucket', 'aws_access_key', 'aws_secret_key']
        elif t == 'local':
            keys = ['path']
        missing = set(keys) - set(v.keys())
        if missing:
            missing = ', '.join(missing)
            raise ImportError('Missing keys for %s: %s.' % (k, missing))

    if reduce(lambda x,y: x+y, config.storage_write_weights.values()) != 1.0:
        msg = 'storage_write_weights values do not add up to 1.0.'
        raise ImportError(msg)

validate_and_set_configuration()

def gen_write_weight_ranges(d):
    total = 0
    r = {}
    for key, val in d.iteritems():
        r[key] = (total, total + val)
        total += val
    return r

write_weight_ranges = None
if getattr(config, 'storage_write_weights', ''):
    write_weight_ranges = gen_write_weight_ranges(config.storage_write_weights)

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
    # Don't set a content_length (that we don't have) to force a chunked
    # transfer.
    conn.put_object(bucket, oops_id, fileobj)
    return True

def write_to_s3(fileobj, oops_id, provider_data):
    '''Write the core file to Amazon S3.'''
    from boto.s3.connection import S3Connection
    from boto.exception import S3ResponseError

    conn = S3Connection(aws_access_key_id=provider_data['aws_access_key'],
                        aws_secret_access_key=provider_data['aws_secret_key'],
                        host=provider_data['host'])
    try:
        bucket = conn.get_bucket(provider_data['bucket'])
    except S3ResponseError as e:
        bucket = conn.create_bucket(provider_data['bucket'])

    key = bucket.new_key(oops_id)
    try:
        key.set_contents_from_stream(fileobj)
    except IOError, e:
        if e.message == 'request data read error':
            return False
        else:
            raise

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
    for key, ranges in write_weight_ranges.iteritems():
        # TODO fix overlap by fixing function above to generate (0, 0.24),
        # (0.25, 0.50), (0.51, 1.0)
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

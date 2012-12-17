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

config = None
try:
    import local_config as config
except ImportError:
    pass
if not config:
    import configuration as config
import os


def submit(_pool, fileobj, uuid, arch, system_hash):
    indexes_fam = pycassa.ColumnFamily(_pool, 'Indexes')
    oops_fam = pycassa.ColumnFamily(_pool, 'OOPS')

    path = os.path.join(config.san_path, uuid)
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

    copied = False
    with open(path, 'w') as fp:
        try:
            shutil.copyfileobj(fileobj, fp, 512)
            copied = True
        except IOError, e:
            if e.message != 'request data read error':
                raise
    if not copied:
        try:
            os.remove(path)
        except OSError:
            pass
        return (False, '')

    os.chmod(path, 0o666)

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
        body = amqp.Message(path)
        # Persistent
        body.properties['delivery_mode'] = 2
        channel.basic_publish(body, exchange='', routing_key=queue)
    finally:
        channel.close()
        connection.close()

    indexes_fam.insert('retracing', {addr_sig : ''})
        
    return (True, uuid)

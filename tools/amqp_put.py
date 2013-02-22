#!/usr/bin/python
import sys
import os
import pycassa
from pycassa.cassandra.ttypes import NotFoundException
import amqplib.client_0_8 as amqp
import atexit

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass
if not configuration:
    from daisy import configuration

if len(sys.argv) < 2:
    print >>sys.stderr, 'usage: %s <uuid>'
    sys.exit(1)

path = sys.argv[1]
uuid = os.path.basename(path)
creds = {'username': configuration.cassandra_username,
         'password': configuration.cassandra_password}
pool = pycassa.ConnectionPool(configuration.cassandra_keyspace,
                              configuration.cassandra_hosts, credentials=creds)
oops_fam = pycassa.ColumnFamily(pool, 'OOPS')
arch = ''
try:
    arch = oops_fam.get(uuid, columns=['Architecture'])['Architecture']
except NotFoundException:
    print >>sys.stderr, 'could not find architecture for %s' % uuid
    sys.exit(1)
queue = 'retrace_%s' % arch

connection = amqp.Connection(host=configuration.amqp_host)
channel = connection.channel()
atexit.register(connection.close)
atexit.register(channel.close)
channel.queue_declare(queue=queue, durable=True, auto_delete=False)
body = amqp.Message(path)
# Persistent
body.properties['delivery_mode'] = 2
channel.basic_publish(body, exchange='', routing_key=queue)
print 'published %s' % path

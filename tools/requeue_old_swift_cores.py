#!/usr/bin/python

# iterate over the core files in swift and if they are rather old assume they
# got dropped from the amqp queue somehow and readd them after looking up the
# arch for the core file in cassandra.

import amqplib.client_0_8 as amqp
import atexit
import pycassa
import swiftclient
import sys

from pycassa.cassandra.ttypes import NotFoundException
from daisy import config
from daisy import utils
from datetime import datetime, timedelta

limit = None
queued_limit = None
if len(sys.argv) == 3:
    limit = int(sys.argv[1])
    queued_limit = int(sys.argv[2])
elif len(sys.argv) == 2:
    limit = int(sys.argv[1])

cs = getattr(config, 'core_storage', '')
if not cs:
    log('core_storage not set.')
    sys.exit(1)

provider_data = cs['swift']
opts = {'tenant_name': provider_data['os_tenant_name'],
        'region_name': provider_data['os_region_name']}
_cached_swift = swiftclient.client.Connection(
    provider_data['os_auth_url'],
    provider_data['os_username'],
    provider_data['os_password'], os_options=opts,
    auth_version='2.0')
bucket = provider_data['bucket']

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                              config.cassandra_hosts, credentials=creds)
oops_fam = pycassa.ColumnFamily(pool, 'OOPS')

_cached_swift.http_conn = None
connection = amqp.Connection(host=config.amqp_host,
                             userid=config.amqp_username,
                             password=config.amqp_password)
channel = connection.channel()
atexit.register(connection.close)
atexit.register(channel.close)

now = datetime.utcnow()
abitago = now - timedelta(14)
count = 0
queued_count = 0


def remove_core(bucket, core):
    try:
        _cached_swift.delete_object(bucket, core)
        print >>sys.stderr, 'removed %s from swift' % core
    except swiftclient.client.ClientException as e:
        if '404 Not Found' in str(e):
            # It may have already been removed
            print >>sys.stderr, '%s not found in swift' % core


for container in _cached_swift.get_container(container=bucket,
                                             limit=limit):
    # the dict is the metadata for the container
    if isinstance(container, dict):
        continue
    for core in container:
        uuid = core['name']
        count += 1
        # The age of the core isn't important when evaluating these.
        arch = ''
        try:
            arch = oops_fam.get(uuid, columns=['Architecture'])['Architecture']
        except NotFoundException:
            print >>sys.stderr, 'could not find architecture for %s' % uuid
            remove_core(bucket, uuid)
            continue
        release = ''
        try:
            release = oops_fam.get(uuid, columns=['DistroRelease'])['DistroRelease']
        except NotFoundException:
            print >>sys.stderr, 'could not find release for %s' % uuid
            remove_core(bucket, uuid)
            continue
        if not utils.retraceable_release(release):
            print >>sys.stderr, 'Unretraceable or EoL release in %s' % uuid
            remove_core(bucket, uuid)
            continue
        failed = False
        try:
            fail_reason = oops_fam.get(uuid, columns=['RetraceFailureReason'])['RetraceFailureReason']
            if fail_reason:
                failed = True
        except NotFoundException:
            pass
        # we already retraced these but the core wasn't removed for some
        # reason
        if failed:
            print >>sys.stderr, 'RetraceFailureReason found for %s' % uuid
            remove_core(bucket, uuid)
            continue
        core_date = datetime.strptime(core['last_modified'],
                                      '%Y-%m-%dT%H:%M:%S.%f')
        # it may still be in the queue awaiting its first retrace attempt
        if core_date > abitago:
            print 'skipping too new core %s' % uuid
            continue
        # don't use resources retrying these arches
        if arch in ['', 'ppc64el', 'arm64', 'armhf']:
            print >>sys.stderr, 'architecture less important for %s' % uuid
            remove_core(bucket, uuid)
            continue
        # Only requeue things if queued_limit is set
        if queued_limit and queued_count < queued_limit:
            queue = 'retrace_%s' % arch
            channel.queue_declare(queue=queue, durable=True, auto_delete=False)
            # msg:provider
            body = amqp.Message('%s:swift' % uuid)
            # Persistent
            body.properties['delivery_mode'] = 2
            channel.basic_publish(body, exchange='', routing_key=queue)
            print 'published %s to %s queue' % (uuid, arch)
            queued_count += 1
        if queued_limit and queued_count >= queued_limit:
            print 'reached limit of cores to queue.'
            break
    print 'Finished, reviewed %i cores.' % count

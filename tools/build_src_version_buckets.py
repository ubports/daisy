#!/usr/bin/python

import pycassa
import uuid
from daisy import config
from collections import Counter

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                              config.cassandra_hosts, timeout=600,
                              max_retries=100, credentials=creds)

oops_cf = pycassa.ColumnFamily(pool, 'OOPS')
srcversbuckets = pycassa.ColumnFamily(pool, 'SourceVersionBuckets')

cols = ['SourcePackage', 'Package', 'DistroRelease']
count = 0
for key, oops in oops_cf.get_range(columns=cols):
    count += 1
    if count % 10000 == 0:
        break
    if count % 100000 == 0:
        print 'processed', count

    if Counter(cols) != Counter(oops.keys()):
        continue

    release = oops['DistroRelease'].encode('utf8')

    if not release.startswith('Ubuntu '):
        continue
    package_data = oops['Package'].split(' ')
    if len(package_data) < 2:
        continue
    version = package_data[1]
    src_package = oops['SourcePackage']
    oops_id = uuid.UUID(key)
    #print('Would insert (%s, %s) = {%s, ""}' % (src_package, version,
    #       oops_id))
    srcversbucketsinsert((src_package, version), {oops_id : ''})

print 'total processed', count

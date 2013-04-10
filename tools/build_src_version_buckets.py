#!/usr/bin/python

import pycassa
import uuid
from daisy import config
from daisy.utils import split_package_and_version
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
    if count % 100000 == 0:
        print 'processed', count

    if Counter(cols) != Counter(oops.keys()):
        continue

    release = oops.get('DistroRelease', '')
    if not release.startswith('Ubuntu ') or release == '':
        continue
    package = oops.get('Package', '')
    if package:
        package, version = split_package_and_version(package)
    src_package = oops.get('SourcePackage', '')
    if src_package == '' or version == '':
        continue
    oops_id = uuid.UUID(key)
    #print('Would insert (%s, %s) = {%s, ""}' % (src_package, version,
    #       oops_id))
    srcversbuckets.insert((src_package, version), {oops_id : ''})

print 'total processed', count

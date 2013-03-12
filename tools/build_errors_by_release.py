#!/usr/bin/python

import datetime
import pycassa
import uuid
from pycassa.cassandra.ttypes import NotFoundException
from daisy import config
from collections import Counter

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                              config.cassandra_hosts, timeout=600,
                              max_retries=100, credentials=creds)

oops_cf = pycassa.ColumnFamily(pool, 'OOPS')
firsterror = pycassa.ColumnFamily(pool, 'FirstError')
errorsbyrelease = pycassa.ColumnFamily(pool, 'ErrorsByRelease')

cols = ['SystemIdentifier', 'DistroRelease']
count = 0
for key, oops in oops_cf.get_range(columns=cols, include_timestamp=True):
    count += 1
    if count % 10000 == 0:
        print 'processed', count

    if Counter(cols) != Counter(oops.keys()):
        continue

    release = oops['DistroRelease'][0].encode('utf8')
    system_token = oops['SystemIdentifier'][0]

    if not release.startswith('Ubuntu '):
        continue

    occurred = oops['DistroRelease'][1] / 1000000
    occurred = datetime.datetime.fromtimestamp(occurred)
    occurred = occurred.replace(hour=0, minute=0, second=0, microsecond=0)

    first_error_date = None
    try:
        first_error_date = firsterror.get(release, columns=[system_token])
        first_error_date = first_error_date[system_token]
    except NotFoundException:
        pass

    if not first_error_date or first_error_date > occurred:
        firsterror.insert(release, {system_token: occurred})
        first_error_date = occurred

    oops_id = uuid.UUID(key)
    errorsbyrelease.insert((release, occurred), {oops_id: first_error_date})

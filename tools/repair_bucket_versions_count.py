#!/usr/bin/python
import sys
import pycassa
from daisy import config
import datetime

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool('crashdb', config.cassandra_hosts, timeout=60,
                              credentials=creds)
bv_full_cf = pycassa.ColumnFamily(pool, 'BucketVersionsFull')
bv_count_cf = pycassa.ColumnFamily(pool, 'BucketVersionsCount')
bv_day_cf = pycassa.ColumnFamily(pool, 'BucketVersionsDay')

def _date_range_iterator(start, finish):
    while start <= finish:
        yield start.strftime('%Y%m%d')
        start += datetime.timedelta(days=1)

today = datetime.datetime.today()
if len(sys.argv) == 2:
    d = datetime.datetime.strptime(sys.argv[1], '%Y%m%d')
else:
    d = today - datetime.timedelta(days=2)

buckets = set()
for x in _date_range_iterator(d, today):
    buckets |= set([k for k,v in bv_day_cf.xget(x)])

i = 0
for bucket in buckets:
    if i % 1000 == 0:
        print i
    bucketid, release, version = bucket
    real_count = bv_full_cf.get_count(bucket)
    actual_count = bv_count_cf.get(bucketid, (release, version))
    c = real_count - actual_count
    if c != 0:
        print bucket, 'adjusted by', c
        bv_count_cf.insert(bucketid, {(release, version): c})
print i

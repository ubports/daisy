#!/usr/bin/python

import sys
import pycassa

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass

if not configuration:
    from daisy import configuration

creds = {'username': configuration.cassandra_username,
         'password': configuration.cassandra_password}
pool = pycassa.ConnectionPool(configuration.cassandra_keyspace,
                              configuration.cassandra_hosts, timeout=600,
                              max_retries=100, credentials=creds)

bucket_cf = pycassa.ColumnFamily(pool, 'Bucket')
buckets_cf = pycassa.ColumnFamily(pool, 'Buckets')

from itertools import izip_longest
def grouper(iterable, n):
    args = [iter(iterable)] * n
    return izip_longest(*args)

row_count = 0
dry_run = '--dry-run' in sys.argv

for k,v in buckets_cf.get_range():
    row_count += 1
    for group in grouper(v, 100):
        # If the list isn't evenly divisible, we'll end up with a final
        # chunk with None values on the end.
        gen = ((pycassa.util.uuid.UUID(x),'') for x in group if x)
        o = pycassa.util.OrderedDict(gen)
        if not dry_run:
            bucket_cf.insert(k, o)
    if row_count % 100000 == 0:
        print 'Copied', row_count, 'rows.'

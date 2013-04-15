#!/usr/bin/python

from __future__ import print_function
import pycassa
import sys

from daisy import config
from daisy.utils import split_package_and_version
from collections import Counter

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                              config.cassandra_hosts, timeout=600,
                              max_retries=100, credentials=creds)

oops_cf = pycassa.ColumnFamily(pool, 'OOPS')
bucket_cf = pycassa.ColumnFamily(pool, 'Bucket')
srcversbuckets = pycassa.ColumnFamily(pool, 'SourceVersionBuckets')

cols = ['SourcePackage', 'Package', 'DistroRelease']
counts = 0

wait_amount = 30000000
wait = wait_amount
start = pycassa.columnfamily.gm_timestamp()

def print_totals(force=False):
    global wait
    if force or (pycassa.columnfamily.gm_timestamp() - start > wait):
        wait += wait_amount
        r = (float(counts) / (pycassa.columnfamily.gm_timestamp() - start) * 1000000 * 60)
        print('Processed:', counts, '(%d/min)' % r, sep='\t')
        print
        sys.stdout.flush()

for bucket, instances in bucket_cf.get_range(include_timestamp=True, buffer_size=2048):
    print_totals()
    str_instances = [str(instance) for instance in instances]
    counts += 1
    if counts > 1000:
        break
    insertions = []
    inserted = False
    for instance in zip(*[iter(str_instances)]*3):
        if inserted:
            continue
        oopses = oops_cf.multiget(instance, columns=cols)
        for oops in oopses:
            data = oopses[oops]
            if Counter(cols) != Counter(data.keys()):
                continue

            release = data.get('DistroRelease', '')
            if not release.startswith('Ubuntu ') or release == '':
                continue
            package = data.get('Package', '')
            if package:
                package, version = split_package_and_version(package)
            src_package = data.get('SourcePackage', '')
            if src_package == '' or version == '':
                continue
            key = (src_package, version)
            if key in insertions:
                inserted = True
                continue
            #print('Would insert %s = {%s, ""}' % (key, bucket))
            insertions.append(key)
            srcversbuckets.insert(key, {bucket: ''})
print_totals(force=True)

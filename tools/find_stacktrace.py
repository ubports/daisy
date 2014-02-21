#!/usr/bin/python
import pycassa
from daisy import config
from collections import Counter, defaultdict
from pycassa.cassandra.ttypes import NotFoundException
import sys

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace, config.cassandra_hosts, timeout=1,
                              pool_size=6, credentials=creds)
oops = pycassa.ColumnFamily(pool, 'OOPS')
bucket = pycassa.ColumnFamily(pool, 'Bucket')
hashes = pycassa.ColumnFamily(pool, 'Hashes')
stack = pycassa.ColumnFamily(pool, 'Stacktrace')
indexes = pycassa.ColumnFamily(pool, 'Indexes')

def main(hashed):
    bucketid = hashes.get('bucket_%s' % hashed[0], columns=[hashed]).values()[0]
    print 'bucket', bucketid

    signatures = [oops.get(str(oopsid)).get('StacktraceAddressSignature')
        for oopsid, _ in bucket.xget(bucketid)]
    print 'considering', len(signatures), 'signatures'
    print_stacktrace(signatures)

def print_stacktrace(signatures):
    for sig in signatures:
        try:
            idx = 'crash_signature_for_stacktrace_address_signature'
            crash_sig = indexes.get(idx, [sig])
            print 'Found crash signature for SAS: %s' % crash_sig
        except NotFoundException:
            pass
        try:
            idx = 'retracing'
            crash_sig = indexes.get(idx, [sig])
            print 'Waiting to retrace SAS: %s' % crash_sig
        except NotFoundException:
            pass
        try:
            print '%s:\n%s' % (sig, stack.get(sig))
        except NotFoundException:
            pass

if __name__ == '__main__':
    hashed = sys.argv[1]
    main(hashed)


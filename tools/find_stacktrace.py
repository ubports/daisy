#!/usr/bin/python
import pycassa
from daisy import config
from pycassa.cassandra.ttypes import NotFoundException
import sys

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                              config.cassandra_hosts, timeout=1,
                              pool_size=6, credentials=creds)
oops = pycassa.ColumnFamily(pool, 'OOPS')
bucket = pycassa.ColumnFamily(pool, 'Bucket')
hashes = pycassa.ColumnFamily(pool, 'Hashes')
stack = pycassa.ColumnFamily(pool, 'Stacktrace')
indexes = pycassa.ColumnFamily(pool, 'Indexes')


def main(hashed):
    try:
        oops_details = oops.get(hashed, ['StacktraceAddressSignature'])
    except NotFoundException:
        print("OOPS instance %s not found" % hashed)
    signature = oops_details['StacktraceAddressSignature']
    print("SAS: %s" % signature)
    print_stacktrace(signature)


def print_stacktrace(signature):
    try:
        idx = 'crash_signature_for_stacktrace_address_signature'
        crash_sig = indexes.get(idx, [signature])
        print('Found crash signature for SAS: %s' %
              crash_sig[signature])
        print('https://errors.ubuntu.com/bucket/?id=%s' %
              crash_sig[signature])
        return
    except NotFoundException:
        pass
    try:
        idx = 'retracing'
        crash_signature = indexes.get(idx, [signature])
        print 'Waiting to retrace SAS: %s' % crash_signature
    except NotFoundException:
        pass
    try:
        print '%s:\n%s' % (signature, stack.get(signature))
    except NotFoundException:
        pass

if __name__ == '__main__':
    hashed = sys.argv[1]
    main(hashed)

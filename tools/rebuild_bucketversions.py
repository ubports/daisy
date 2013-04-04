#!/usr/bin/python

from __future__ import print_function
import apport
import sys
import pycassa
from pycassa.cassandra.ttypes import NotFoundException
from daisy.utils import split_package_and_version
from daisy import config
from collections import defaultdict
import argparse
import uuid

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                              config.cassandra_hosts, timeout=60,
                              pool_size=15, max_retries=100, credentials=creds)

oops_cf = pycassa.ColumnFamily(pool, 'OOPS')
indexes_cf = pycassa.ColumnFamily(pool, 'Indexes')
awaiting_retrace_cf = pycassa.ColumnFamily(pool, 'AwaitingRetrace')
bv_full_cf = None
bv_day_cf = None

counts = defaultdict(int)

wait_amount = 30000000
wait = wait_amount

def print_totals(force=False):
    global wait
    if force or (pycassa.columnfamily.gm_timestamp() - start > wait):
        wait += wait_amount
        t = float(counts['binary'] + counts['python'] + counts['dups'] +
                  counts['no_sas'] + counts['no_signature'])
        r = (t / (pycassa.columnfamily.gm_timestamp() - start) * 1000000 * 60)
        print('Processed:', t, '(%d/min)' % r, sep='\t')
        for k in counts:
            print('%s:' % k, counts[k], sep='\t')
        print
        sys.stdout.flush()

def update_bucketversions(bucketid, oops, key):
    global bv_full_cf
    global bv_day_cf

    if 'ProblemType' not in oops:
        return

    key = uuid.UUID(key)
    version = ''
    package = oops.get('Package', '')
    release = oops.get('DistroRelease', '')
    # These are tuples of (value, timestamp)
    if release:
        release = release[0]
    if package:
        package = package[0]
        package, version = split_package_and_version(package)

    if not package:
        counts['no_package'] += 1
    if not release:
        counts['no_release'] += 1

    if bv_full_cf:
        bv_full_cf.insert((bucketid, release, version), {key: ''})

    if bv_day_cf:
        ts = oops['ProblemType'][1]
        day_key = time.strftime('%Y%m%d', time.gmtime(ts / 1000000))
        bv_day_cf.insert(day_key, {(bucketid, release, version): ''})

idx_key = 'crash_signature_for_stacktrace_address_signature'
crash_sigs = {k:v for k,v in indexes_cf.xget(idx_key)}

start = pycassa.columnfamily.gm_timestamp()

# We don't need Stacktrace or ThreadStacktrace or any of that because we get
# the crash signature from *just* the SAS for binary crashes.
columns = ['ExecutablePath', 'Traceback', 'ProblemType', 'DuplicateSignature',
           'StacktraceAddressSignature', 'DistroRelease', 'Package',
           'InterpreterPath', 'OopsText']
columns.sort()

kwargs = {
    'include_timestamp': True,
    'buffer_size': (1024*4),
    'columns': columns,
}

def handle_duplicate_signature(key, o):
    ds = o['DuplicateSignature'][0].encode('utf-8')
    update_bucketversions(ds, o, key)
    counts['dups'] += 1

def handle_python(key, o):
    report = apport.Report()
    for k in o:
        report[k.encode('utf-8')] = o[k][0].encode('utf-8')
    crash_signature = report.crash_signature()
    if not crash_signature:
        if 'Stacktrace' not in o:
            counts['no_python_signature'] += 1
            return
    update_bucketversions(crash_signature, o, key)
    counts['python'] += 1

def handle_binary(key, o):
    addr_sig = o.get('StacktraceAddressSignature', None)
    if not addr_sig:
        counts['no_sas'] += 1
        return
    if not addr_sig[0]:
        counts['empty_sas'] += 1
        return

    addr_sig = addr_sig[0]
    crash_sig = crash_sigs.get(addr_sig, None)

    if crash_sig:
        update_bucketversions(crash_sig, o, key)
        counts['binary'] += 1
    else:
        # If we cannot find the address signature, it may have been bucketed
        # while this program was running. We do not need to look up the address
        # signature in the actual indexes CF though, as the new daisy code
        # would have already written this to bucketversions.
        counts['no_signature'] += 1
        #count_retracing(addr_sig, key, o)

def count_retracing(addr_sig, key, o):
    try:
        indexes_cf.get('retracing', [addr_sig])
        counts['retracing'] += 1
    except NotFoundException:
        counts['not_retracing'] += 1
        try:
            awaiting_retrace_cf.get(addr_sig, [key])
            counts['awaiting_retrace'] += 1
        except NotFoundException:
            counts['not_awaiting_retrace'] += 1

def main():
    for key, o in oops_cf.get_range(**kwargs):
        print_totals()

        if 'DuplicateSignature' in o:
            handle_duplicate_signature(key, o)
        elif 'InterpreterPath' in o and 'StacktraceAddressSignature' not in o:
            # FIXME better check here and in the real code. We might not have an
            # SAS.
            handle_python(key, o)
        elif 'StacktraceAddressSignature' in o:
            handle_binary(key, o)
        else:
            counts['unknown'] += 1

    print_totals(force=True)

def parse_options():
    parser = argparse.ArgumentParser(description='Rebuild BucketVersions.')
    parser.add_argument('--write-hosts', nargs='+',
                        help='Cassandra host and IP (colon-separated) to write'
                        ' results to.')
    return parser.parse_args()

if __name__ == '__main__':
    options = parse_options()
    if options.write_hosts:
        write_pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                                            options.write_hosts, timeout=60,
                                            pool_size=15, max_retries=100,
                                            credentials=creds)
        bv_full_cf = pycassa.ColumnFamily(write_pool, 'BucketVersionsFull')
        bv_day_cf = pycassa.ColumnFamily(write_pool, 'BucketVersionsDay')

    main()

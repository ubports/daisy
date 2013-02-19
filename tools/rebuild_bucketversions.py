#!/usr/bin/python

import apport
import sys
import pycassa
from pycassa.cassandra.ttypes import NotFoundException, InvalidRequestException
from utils import split_package_and_version

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass

if not configuration:
    import configuration

creds = {'username': configuration.cassandra_username,
         'password': configuration.cassandra_password}
pool = pycassa.ConnectionPool(configuration.cassandra_keyspace,
                              configuration.cassandra_hosts, timeout=600,
                              max_retries=100, credentials=creds)

oops_cf = pycassa.ColumnFamily(pool, 'OOPS')
oops_cf = pycassa.ColumnFamily(pool, 'OOPS')
indexes_fam = pycassa.ColumnFamily(pool, 'Indexes')
#bucketversion_cf = pycassa.ColumnFamily(pool, 'BucketVersion')

no_sas = 0
no_signature = 0

dups = 0
python = 0
binary = 0

start = pycassa.columnfamily.gm_timestamp()

no_package = 0
no_release = 0

retracing = 0
not_retracing = 0

wait_amount = 300000000 # 5 minutes
wait = wait_amount

def print_totals(force=False):
    global no_sas
    global no_signature
    global no_package
    global no_release
    global dups
    global python
    global binary
    global wait
    global retracing
    global not_retracing

    if force or (pycassa.columnfamily.gm_timestamp() - start > wait):
        wait += wait_amount
        print 'binary:', binary
        print 'python:', python
        print 'dups:', dups
        print 'no_sas:', no_sas
        print 'no_signature:', no_signature
        print 'no_package:', no_package
        print 'no_release:', no_release
        print 'retracing:', retracing
        print 'not_retracing:', not_retracing
        print

def update_bucketversions(bucketid, oops):
    global no_package
    global no_release

    if 'ProblemType' not in oops or oops['ProblemType'][1] > start:
        # This has come in since this script started running, and will have
        # been correctly bucketed.
        return
    package = oops.get('Package', '')
    if package:
        package = split_package_and_version(package[0])
    release = oops.get('DistroRelease', '')
    if release:
        release = release[0]

    if not package:
        no_package += 1
        return

    if not release:
        no_release += 1

    # bucketversion_cf.add(bucketid, (package, release))

for key, o in oops_cf.get_range(include_timestamp=True):
    report = apport.Report()
    if 'DuplicateSignature' in o:
        ds = o['DuplicateSignature'][0].encode('utf-8')
        update_bucketversions(ds, o)
        dups += 1
        continue
    elif 'InterpreterPath' in o and not 'StacktraceAddressSignature' in o:
        for key in o:
            report[key.encode('utf-8')] = o[key][0].encode('utf-8')
        crash_signature = report.crash_signature()
        update_bucketversions(crash_signature, o)
        python += 1
        continue
    addr_sig = o.get('StacktraceAddressSignature', None)
    if addr_sig:
        addr_sig = addr_sig[0]
    else:
        no_sas += 1
        continue

    crash_sig = None
    try:
        s = 'crash_signature_for_stacktrace_address_signature'
        crash_sig = indexes_fam.get(s, [addr_sig])
        crash_sig = crash_sig[addr_sig]
    except NotFoundException:
        no_signature += 1
        try:
            indexes_fam.get('retracing', [addr_sig])
            retracing += 1
        except NotFoundException:
            not_retracing += 1
        continue
    if crash_sig:
        update_bucketversions(crash_sig, o)
        binary += 1
    else:
        no_signature += 1

    print_totals()

print_totals(force=True)

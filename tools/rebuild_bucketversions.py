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
pool = pycassa.ConnectionPool(configuration.cassandra_keyspace, pool_size=15,
                              configuration.cassandra_hosts, timeout=60,
                              max_retries=100, credentials=creds)

oops_cf = pycassa.ColumnFamily(pool, 'OOPS')
indexes_fam = pycassa.ColumnFamily(pool, 'Indexes')
#awaiting_retrace_cf = pycassa.ColumnFamily(pool, 'AwaitingRetrace')
#bucketversion_cf = pycassa.ColumnFamily(pool, 'BucketVersion',
#                                        retry_counter_mutations=True)

no_sas = 0
no_signature = 0

dups = 0
python = 0
binary = 0

start = 0

no_package = 0
no_release = 0

retracing = 0
not_retracing = 0

#awaiting_retrace = 0
#not_awaiting_retrace = 0

no_python_signature = 0

wait_amount = 30000000
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
    #global awaiting_retrace
    #global not_awaiting_retrace
    global no_python_signature

    if force or (pycassa.columnfamily.gm_timestamp() - start > wait):
        wait += wait_amount
        t = float(binary + python + dups + no_sas + no_signature)
        r = (t / (pycassa.columnfamily.gm_timestamp() - start) * 1000000 * 60)
        print 'Processed:', t, '(%d/min)' % r
        print 'binary:', binary
        print 'python:', python
        print 'dups:', dups
        print 'no_sas:', no_sas
        print 'no_signature:', no_signature
        print 'no_package:', no_package
        print 'no_release:', no_release
        print 'retracing:', retracing
        print 'not_retracing:', not_retracing
        #print 'awaiting_retrace:', awaiting_retrace
        #print 'not_awaiting_retrace:', not_awaiting_retrace
        print 'no_python_signature:', no_python_signature
        print
        sys.stdout.flush()

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

idx_key = 'crash_signature_for_stacktrace_address_signature' 
crash_sigs = {k:v for k,v in indexes_fam.xget(idx_key)}

start = pycassa.columnfamily.gm_timestamp()

for key, o in oops_cf.get_range(**kwargs):
    print_totals()
    if 'DuplicateSignature' in o:
        ds = o['DuplicateSignature'][0].encode('utf-8')
        update_bucketversions(ds, o)
        dups += 1
        continue
    elif 'InterpreterPath' in o and 'StacktraceAddressSignature' not in o:
        report = apport.Report()
        for k in o:
            report[k.encode('utf-8')] = o[k][0].encode('utf-8')
        crash_signature = report.crash_signature()
        if not crash_signature:
            if 'Stacktrace' not in o:
                #print 'COULD NOT GENERATE PYTHON SIGNATURE', key
                no_python_signature += 1
                continue
        update_bucketversions(crash_signature, o)
        python += 1
        continue
    addr_sig = o.get('StacktraceAddressSignature', None)
    if addr_sig:
        addr_sig = addr_sig[0]
        if not addr_sig:
            # TODO BUT WHY IS THIS HAPPENING?
            print 'EMPTY SAS', key
            no_sas += 1
            continue
    else:
        no_sas += 1
        continue

    crash_sig = crash_sigs.get(addr_sig, None)
    # If we cannot find the address signature, it may have been bucketed while
    # this program was running. We do not need to look up the address signature
    # in the actual indexes CF though, as the new daisy code would have already
    # written this to bucketversions.

    if crash_sig:
        update_bucketversions(crash_sig, o)
        binary += 1
    else:
        no_signature += 1
        #try:
        #    indexes_fam.get('retracing', [addr_sig])
        #    retracing += 1
        #except NotFoundException:
        #    not_retracing += 1
        #    try:
        #        awaiting_retrace_cf.get(addr_sig, [key])
        #        awaiting_retrace += 1
        #    except NotFoundException:
        #        if 'Stacktrace' not in o:
        #            print 'NOT AWAITING RETRACE', key
        #        not_awaiting_retrace += 1


print_totals(force=True)

#!/usr/bin/python

from __future__ import print_function
import pycassa
import sys

from daisy import config
from pycassa.cassandra.ttypes import NotFoundException

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                              config.cassandra_hosts, timeout=600,
                              max_retries=100, credentials=creds)

oops_cf = pycassa.ColumnFamily(pool, 'OOPS')
indexes_cf = pycassa.ColumnFamily(pool, 'Indexes')
awaiting_retrace_cf = pycassa.ColumnFamily(pool, 'AwaitingRetrace')

unneeded_columns = ['Disassembly', 'ProcMaps', 'ProcStatus',
                    'Registers', 'StacktraceTop', 'Stacktrace',
                    'ThreadStacktrace']
counts = 0

wait_amount = 30000000
wait = wait_amount
start = pycassa.columnfamily.gm_timestamp()


def print_totals(force=False):
    global wait
    if force or (pycassa.columnfamily.gm_timestamp() - start > wait):
        wait += wait_amount
        r = (float(counts) / (pycassa.columnfamily.gm_timestamp() - start)
             * 1000000 * 60)
        print('Processed:', counts, '(%d/min)' % r, sep='\t')
        print
        sys.stdout.flush()

for oops_id, data in oops_cf.get_range(columns=['StacktraceAddressSignature'],
                                            buffer_size=2*1024):
    signature = data['StacktraceAddressSignature']
    print_totals()
    counts += 1
    if signature.startswith('failed:'):
        print("%s failed to retrace" % oops_id)
        continue
    try:
        awaiting = awaiting_retrace_cf.get(signature)
        print("%s is waiting to retrace" % oops_id)
        continue
    except NotFoundException:
        pass
    try:
        idx = 'retracing'
        crash_signature = indexes_cf.get(idx, [signature])
        print("%s is retracing" % oops_id)
        continue
    except NotFoundException:
        pass
    print('Removing data for %s' % oops_id)
    #if counts >= 5:
    #    break
    oops_cf.remove(oops_id, columns=unneeded_columns)
print_totals(force=True)

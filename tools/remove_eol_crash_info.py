#!/usr/bin/python

# Review crashes in the OOPS table for any from an End of Life release and
# remove unneeded_columns from those OOPSes.


from __future__ import print_function
import pycassa
import sys

from daisy import config

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                              config.cassandra_hosts, timeout=600,
                              max_retries=100, credentials=creds)

oops_cf = pycassa.ColumnFamily(pool, 'OOPS')

never_needed_columns = ['Stacktrace', 'ThreadStacktrace']
unneeded_columns = ['Disassembly', 'ProcMaps', 'ProcStatus',
                    'Registers', 'StacktraceTop']
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
# start is the first OOPs ID to check
for oops in oops_cf.get_range(start='79be7ecc-7e63-11e4-b370-fa163e22e467',
        columns=['DistroRelease'],
        buffer_size=2*1024):
    eol = False
    oops_id = oops[0]
    data = oops[1]
    release = data.get('DistroRelease', '')
    print_totals()
    counts += 1
    # Stacktrace and ThreadStacktrace should not be in the OOPS table as
    # submit.py removes them, but try to remove it just in case.
    oops_cf.remove(oops_id, columns=never_needed_columns)
    # For EoL releases remove the columns that a developer would not want to see.
    if release in ['Ubuntu 12.10', 'Ubuntu 13.04', 'Ubuntu 13.10', 'Ubuntu 14.10']:
        eol = True
        print('Data removed for EoL release %s http://errors.ubuntu.com/oops/%s' %
              (release.strip('Ubuntu '), oops_id))
        oops_cf.remove(oops_id, columns=unneeded_columns)
    # For testing. ;-)
    # if counts >= 100:
    #      break
print_totals(force=True)
print("Done!")

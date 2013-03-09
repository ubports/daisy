#!/usr/bin/python

import sys
import pycassa
from apport import report
from daisy import config

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                              config.cassandra_hosts, timeout=600,
                              credentials=creds)

oops_cf = pycassa.ColumnFamily(pool, 'OOPS')


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print 'usage: oops file-path [core]'
        sys.exit(1)
    core = None
    if len(sys.argv) == 4:
        core = sys.argv[3]
    oops = oops_cf.get(sys.argv[1])
    r = report.Report()
    for k in oops:
        r[k] = oops[k]
    if core:
        r['CoreDump'] = (core,)
    fp = open(sys.argv[2], 'wb')
    r.write(fp)

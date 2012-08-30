#!/usr/bin/python

import sys
import pycassa
from pycassa.cassandra.ttypes import NotFoundException, InvalidRequestException
from apport import report


configuration = None
try:
    import local_config as configuration
except ImportError:
    pass

if not configuration:
    import configuration

pool = pycassa.ConnectionPool(configuration.cassandra_keyspace,
                              configuration.cassandra_hosts, timeout=600)

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

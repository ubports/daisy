#!/usr/bin/python

import os
import pycassa
import sys

from apport import report
from daisy import config
from subprocess import Popen, PIPE

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
        core_file = sys.argv[3]
    oops = oops_cf.get(sys.argv[1])
    report = report.Report()
    for k in oops:
        report[k] = oops[k]
    from ipdb import set_trace; set_trace()
    if core_file:
        with open(core_file.replace('core', 'coredump'), 'wb') as fp:
        #r['CoreDump'] = (core,)
            # 2014-02-11 the coredumps fo0bar gave me were already base64
            # decoded, if they aren't in the future then you'll need this
            #p1 = Popen(['base64', '-d', core_file], stdout=PIPE)
            # Set stderr to PIPE so we get output in the result tuple.
            #p2 = Popen(['zcat'], stdin=p1.stdout, stdout=fp, stderr=PIPE)
            p2 = Popen(['zcat', core_file], stdout=fp, stderr=PIPE)
            ret = p2.communicate()
        report['CoreDump'] = (core_file.replace('core', 'coredump'),)
    fp = open(sys.argv[2], 'wb')
    report.write(fp)
    os.remove(core_file.replace('core', 'coredump'))

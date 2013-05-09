#!/usr/bin/python

import sys
import pycassa
from pycassa.cassandra.ttypes import NotFoundException
import datetime
from daisy import config

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                              config.cassandra_hosts, timeout=600,
                              credentials=creds)

ttr = pycassa.ColumnFamily(pool, 'TimeToRetrace')

# Main

def main():
    d = datetime.date.today().strftime('%Y%m%d')
    l = [v for k, v in ttr.xget(d)]
    count = len(l)
    if count > 0:
        m = sum(l) / count
        if m > config.time_to_retrace_alert:
            print 'Retracers are taking too long to process:'
            msg = 'Currently: %d. Maximum: %d (config.time_to_retrace_alert)'
            print msg % (m, config.time_to_retrace_alert)
            # Nagios uses exit code 1 for WARNING and 2 for CRITICAL.
            sys.exit(2)

if __name__ == '__main__':
    main()

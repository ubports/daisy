#!/usr/bin/python

import sys
import pycassa
import datetime
from daisy import config

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                              config.cassandra_hosts, timeout=600,
                              credentials=creds)

dayoops_cf = pycassa.ColumnFamily(pool, 'DayOOPS')
dayusers_cf = pycassa.ColumnFamily(pool, 'DayUsers')
oops_cf = pycassa.ColumnFamily(pool, 'OOPS')

# Utilities

def _date_range_iterator(start, finish):
    # Iterate all the values including and between the start and finish date
    # string.
    while start <= finish:
        yield start.strftime('%Y%m%d')
        start += datetime.timedelta(days=1)

# Main
if __name__ == '__main__':
    if len(sys.argv) > 2:
        start = datetime.datetime.strptime(sys.argv[1], '%Y%m%d')
        end = datetime.datetime.strptime(sys.argv[2], '%Y%m%d')
        i = _date_range_iterator(start, end)
    else:
        today = datetime.date.today()
        i = _date_range_iterator(today - datetime.timedelta(days=90), today)
    for date in i:
        print date, dayusers_cf.get_count('Ubuntu 12.04:%s' % date)

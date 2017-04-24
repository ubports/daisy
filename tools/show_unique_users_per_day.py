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

dayusers_cf = pycassa.ColumnFamily(pool, 'DayUsers')

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
        i = _date_range_iterator(today - datetime.timedelta(days=10), today)
    print "Date, 12.04, 14.04, 14.10, 15.04, 15.10, 16.04, 16.10, 17.04, Total"
    for date in i:
        precise = dayusers_cf.get_count('Ubuntu 12.04:%s' % date)
        trusty = dayusers_cf.get_count('Ubuntu 14.04:%s' % date)
        utopic = dayusers_cf.get_count('Ubuntu 14.10:%s' % date)
        vivid = dayusers_cf.get_count('Ubuntu 15.04:%s' % date)
        wily = dayusers_cf.get_count('Ubuntu 15.10:%s' % date)
        xenial = dayusers_cf.get_count('Ubuntu 16.04:%s' % date)
        yakkety = dayusers_cf.get_count('Ubuntu 16.10:%s' % date)
        zesty = dayusers_cf.get_count('Ubuntu 17.04:%s' % date)
        total = (precise + trusty + utopic + vivid + wily + xenial +
                 yakkety + zesty)
        print("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s" % \
              (date, precise, trusty, utopic, vivid, wily,
               xenial, yakkety, zesty, total))

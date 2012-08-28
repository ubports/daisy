#!/usr/bin/python

import sys
import time
import pycassa
from pycassa.cassandra.ttypes import NotFoundException, InvalidRequestException
import datetime
import itertools

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass

if not configuration:
    import configuration

pool = pycassa.ConnectionPool(configuration.cassandra_keyspace,
                              configuration.cassandra_hosts, timeout=600)

dayusers_cf = pycassa.ColumnFamily(pool, 'DayUsers')

def _date_range_iterator(start, finish):
    # Iterate all the values including and between the start and finish date
    # string.
    while start <= finish:
        yield start.strftime('%Y%m%d')
        start += datetime.timedelta(days=1)

# Main

if __name__ == '__main__':
    if sys.argv > 1:
        start = datetime.datetime.strptime(sys.argv[1], '%Y%m%d')
    else:
        start = datetime.date.today()
    i = _date_range_iterator(start - datetime.timedelta(days=90), start)
    users = set()
    for date in i:
        start = ''
        while True:
            try:
                buf = dayusers_cf.get('Ubuntu 12.04:%s' % date, column_start=start, column_count=1000)
            except NotFoundException:
                break
            buf = buf.keys()
            start = buf[-1]
            users.update(buf)
            if len(buf) < 1000:
                break
    print 'total unique users in 90 days:', len(users)

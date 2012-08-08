#!/usr/bin/python

import pycassa
from pycassa.cassandra.ttypes import NotFoundException, InvalidRequestException
import datetime

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass

if not configuration:
    import configuration

pool = pycassa.ConnectionPool(configuration.cassandra_keyspace,
                              [configuration.cassandra_host], timeout=15)

dayusers_cf = pycassa.ColumnFamily(pool, 'DayUsers')

def _date_range_iterator(start, finish):
    # Iterate all the values including and between the start and finish date
    # string.
    while start <= finish:
        yield start.strftime('%Y%m%d')
        start += datetime.timedelta(days=1)

if __name__ == '__main__':
    total = set()
    today = datetime.date.today()
    i = _date_range_iterator(today - datetime.timedelta(days=90), today)
    for date in i:
        print 'looking up', date
        start = ''
        while True:
            try:
                buf = dayusers_cf.get(date, column_start=start, column_count=1000)
            except NotFoundException:
                break
            buf = buf.keys()
            total.update(set(buf))
            start = buf[-1]
            if len(buf) < 1000:
                break
    print len(total)

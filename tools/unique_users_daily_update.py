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

uniqueusers_cf = pycassa.ColumnFamily(pool, 'UniqueUsers90Days')
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
    if len(sys.argv) == 3:
        d = datetime.datetime.strptime(sys.argv[2], '%Y%m%d')
        formatted = sys.argv[2]
    elif len(sys.argv) == 2:
        # Yesterday
        d = datetime.datetime.today() - datetime.timedelta(days=1)
        formatted = d.strftime('%Y%m%d')
    else:
        print >>sys.stderr, "Usage: release_name [date]"
        sys.exit(1)
    release = sys.argv[1]
    i = _date_range_iterator(d - datetime.timedelta(days=89), d)
    users = set()
    for date in i:
        start = ''
        while True:
            try:
                buf = dayusers_cf.get('%s:%s' % (release, date), column_start=start, column_count=1000)
            except NotFoundException:
                break
            buf = buf.keys()
            start = buf[-1]
            users.update(buf)
            if len(buf) < 1000:
                break
    uniqueusers_cf.insert(release, {formatted: len(users)})

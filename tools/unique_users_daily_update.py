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
    if '--dry-run' in sys.argv:
        dry_run = True
    else:
        dry_run = False
    if len(sys.argv) > 2:
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
    c = 0
    for date in i:
        if dry_run:
            print 'looking up', date,
            c += 1
        start = ''
        u = 0
        while True:
            try:
                buf = dayusers_cf.get('%s:%s' % (release, date), column_start=start, column_count=1000)
            except NotFoundException:
                break
            buf = buf.keys()
            u += len(buf)
            start = buf[-1]
            users.update(buf)
            if len(buf) < 1000:
                break
        if dry_run:
            print u
    if not dry_run:
        try:
            print 'Was', uniqueusers_cf.get(release, columns=[formatted])
        except NotFoundException:
            pass
        l = len(users)
        print 'Now', l
        uniqueusers_cf.insert(release, {formatted: l})
    else:
        print release + ':', len(users)
    print 'from', c, 'days'

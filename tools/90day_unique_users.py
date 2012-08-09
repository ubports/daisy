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

dayoops_cf = pycassa.ColumnFamily(pool, 'DayOOPS')
oops_cf = pycassa.ColumnFamily(pool, 'OOPS')

def _date_range_iterator(start, finish):
    # Iterate all the values including and between the start and finish date
    # string.
    while start <= finish:
        yield start.strftime('%Y%m%d')
        start += datetime.timedelta(days=1)

if __name__ == '__main__':
    today = datetime.date.today()
    i = _date_range_iterator(today - datetime.timedelta(days=90), today)
    total = []
    no_identifier = 0
    for date in i:
        print 'looking up', date
        oopses = set()
        start = pycassa.util.convert_time_to_uuid(0)
        while True:
            try:
                buf = dayoops_cf.get(date, column_start=start, column_count=1000)
            except NotFoundException:
                break
            start = buf.keys()[-1]
            buf = buf.values()
            oopses.update(set(buf))
            if len(buf) < 1000:
                break
        for oops in oopses:
            try:
                d = oops_cf.get(oops, columns=['DistroRelease', 'SystemIdentifier'])
            except NotFoundException:
                continue
            if 'DistroRelease' in d and d['DistroRelease'] == 'Ubuntu 12.04':
                try:
                    total.append(d['SystemIdentifier'])
                except KeyError:
                    no_identifier += 1
    total = set(total)
    print 'total', len(total)
    print 'no identifier', no_identifier

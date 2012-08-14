#!/usr/bin/python

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
                              [configuration.cassandra_host], timeout=600)

dayoops_cf = pycassa.ColumnFamily(pool, 'DayOOPS')
dayusers_cf = pycassa.ColumnFamily(pool, 'DayUsers')
oops_cf = pycassa.ColumnFamily(pool, 'OOPS')

# Utilities

class Timer(object):
    def __init__(self, name=None):
        self.name = name
    
    def __enter__(self):
        self.tstart = time.time()
        
    def __exit__(self, type, value, traceback):
        if self.name:            
            print '[%s]' % self.name,
        print 'Elapsed: %s' % (time.time() - self.tstart)

def _date_range_iterator(start, finish):
    # Iterate all the values including and between the start and finish date
    # string.
    while start <= finish:
        yield start.strftime('%Y%m%d')
        start += datetime.timedelta(days=1)

# Main

def fetch_oopses(date):
    oopses = []
    start = pycassa.util.convert_time_to_uuid(0)
    while True:
        try:
            buf = dayoops_cf.get(date, column_start=start, column_count=1000)
        except NotFoundException:
            break
        start = buf.keys()[-1]
        buf = buf.values()
        oopses.extend(buf)
        if len(buf) < 1000:
            break
    return oopses

def fetch_identifiers(oopses):
    kwargs = dict(
        columns=['DistroRelease', 'SystemIdentifier'],
        buffer_size=25,
        read_consistency_level=pycassa.ConsistencyLevel.ONE
    )
    # The buffer size here needs to be carefully tuned. If set too high, it
    # will greatly overwhelm Cassandra. Watch nodetool closely when modifying.
    gen = ((d['SystemIdentifier'], '')
            for d in oops_cf.multiget(oopses, **kwargs).values()
                if 'SystemIdentifier' in d
                    and d.get('DistroRelease', '') == 'Ubuntu 12.04')
    return gen

if __name__ == '__main__':
    today = datetime.date.today()
    i = _date_range_iterator(today - datetime.timedelta(days=90), today)
    for date in i:
        print 'looking up', date

        with Timer('fetching oopses'):
            oopses = fetch_oopses(date)
        print 'oopses:', len(oopses)

        if not oopses:
            continue

        with Timer('fetching identifiers'):
            ids = fetch_identifiers(oopses)

        with Timer('inserting identifiers'):
            args = [iter(ids)] * 100
            for k in itertools.izip_longest(ids):
                dayusers_cf.insert('Ubuntu 12.04:%s' % date,
                                   pycassa.util.OrderedDict(k))
        print

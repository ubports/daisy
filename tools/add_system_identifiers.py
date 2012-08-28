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
                              configuration.cassandra_hosts, timeout=15)

useroops_cf = pycassa.ColumnFamily(pool, 'UserOOPS')
oops_cf = pycassa.ColumnFamily(pool, 'OOPS')

if __name__ == '__main__':
    for key, d in useroops_cf.get_range():
        for oops in d.keys():
            oops_cf.insert(oops, {'SystemIdentifier' : key })

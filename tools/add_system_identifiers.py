#!/usr/bin/python

import pycassa

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass

if not configuration:
    from daisy import configuration

creds = {'username': configuration.cassandra_username,
         'password': configuration.cassandra_password}
pool = pycassa.ConnectionPool(configuration.cassandra_keyspace,
                              configuration.cassandra_hosts, timeout=15,
                              credentials=creds)

useroops_cf = pycassa.ColumnFamily(pool, 'UserOOPS')
oops_cf = pycassa.ColumnFamily(pool, 'OOPS')

if __name__ == '__main__':
    for key, d in useroops_cf.get_range():
        for oops in d.keys():
            oops_cf.insert(oops, {'SystemIdentifier' : key })

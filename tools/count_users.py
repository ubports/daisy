#!/usr/bin/python
import pycassa
from pycassa.cassandra.ttypes import NotFoundException
configuration = None
try:
    import local_config as configuration
except ImportError:
        pass
if not configuration:
    from daisy import configuration

creds = {'username': configuration.cassandra_username,
         'password': configuration.cassandra_password}
pool = pycassa.ConnectionPool('crashdb', ['localhost'], timeout=10,
                              credentials=creds)
oops = pycassa.ColumnFamily(pool, 'UserOOPS')

count = 0
for x in oops.get_range(column_count=1):
    count += 1
print count

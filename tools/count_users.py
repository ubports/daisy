import pycassa
from pycassa.cassandra.ttypes import NotFoundException
pool = pycassa.ConnectionPool('crashdb', ['localhost'], timeout=10)
oops = pycassa.ColumnFamily(pool, 'UserOOPS')

count = 0
for x in oops.get_range(column_count=1):
    count += 1
print count

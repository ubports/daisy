#!/usr/bin/python
import pycassa
from pycassa.cassandra.ttypes import NotFoundException, InvalidRequestException

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass
if not configuration:
    import configuration

pool = pycassa.ConnectionPool(configuration.cassandra_keyspace,
                              configuration.cassandra_hosts, timeout=15)
bucketmetadata_cf = pycassa.ColumnFamily(pool, 'BucketMetadata')

def main():
    for key, column_data in bucketmetadata_cf.get_range(columns=['LastSeen', 'FirstSeen']):
        if column_data['LastSeen'] == '(not' and key:
            print 'fixing', key
            bucketmetadata_cf.insert(key, {'LastSeen':''})
        if column_data['FirstSeen'] == '(not' and key:
            print 'fixing', key
            bucketmetadata_cf.insert(key, {'FirstSeen':''})

if __name__ == '__main__':
    main()

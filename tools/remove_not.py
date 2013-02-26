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

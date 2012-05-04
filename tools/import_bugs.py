#!/usr/bin/python
import pycassa
from pycassa.cassandra.ttypes import NotFoundException, InvalidRequestException
import sqlite3
import sys

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass
if not configuration:
    import configuration

pool = pycassa.ConnectionPool(configuration.cassandra_keyspace,
                              [configuration.cassandra_host], timeout=15)
bucketmetadata_cf = pycassa.ColumnFamily(pool, 'BucketMetadata')

def import_bug_numbers (path):
    connection = sqlite3.connect(path)
    # The apport duplicates database mysteriously has lots of dpkg logs in it.
    sql = 'select crash_id, signature from crashes where signature not like ?'
    for crash_id, signature in connection.execute(sql, ('%%\n%%',)):
        bucketmetadata_cf.insert(signature.encode('utf-8'),
                                 {'LaunchpadBug': str(crash_id)})

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print >>sys.stderr, 'Usage: %s <apport_duplicates.db>' % sys.argv[0]
        sys.exit(1)
    import_bug_numbers(sys.argv[1])

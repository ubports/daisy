#!/usr/bin/python
import pycassa
import sqlite3
import sys
from daisy import config

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                              config.cassandra_hosts, timeout=15,
                              credentials=creds)
bucketmetadata_cf = pycassa.ColumnFamily(pool, 'BucketMetadata')
bugtocrashsignatures_cf = pycassa.ColumnFamily(pool, 'BugToCrashSignatures')

def import_bug_numbers (path):
    connection = sqlite3.connect(path)
    # The apport duplicates database mysteriously has lots of dpkg logs in it.
    sql = 'select crash_id, signature from crashes where signature not like ?'
    for crash_id, signature in connection.execute(sql, ('%%\n%%',)):
        bucketmetadata_cf.insert(signature.encode('utf-8'),
                                 {'LaunchpadBug': str(crash_id)})
        bugstocrashsignatures_cf.insert(int(crash_id),
                                        {signature.encode('utf-8'): ''})

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print >>sys.stderr, 'Usage: %s <apport_duplicates.db>' % sys.argv[0]
        sys.exit(1)
    import_bug_numbers(sys.argv[1])

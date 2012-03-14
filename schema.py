from pycassa.system_manager import (
    SystemManager,
    UTF8_TYPE,
    )
import configuration
from oopsrepository.cassandra import workaround_1779

def create():
    keyspace = configuration.cassandra_keyspace
    mgr = SystemManager()
    try:
        workaround_1779(mgr.create_column_family, keyspace, 'Indexes',
            comparator_type=UTF8_TYPE)
        workaround_1779(mgr.create_column_family, keyspace, 'Stacktrace',
            comparator_type=UTF8_TYPE)
        workaround_1779(mgr.create_column_family, keyspace, 'AwaitingRetrace',
            comparator_type=UTF8_TYPE)
    finally:
        mgr.close()

if __name__ == '__main__':
    create()

import uuid
import hashlib
import datetime
import random

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
                              [configuration.cassandra_host], timeout=30)
oops_cf = pycassa.ColumnFamily(pool, 'OOPS')
dayoops_cf = pycassa.ColumnFamily(pool, 'DayOOPS')

release = 'Ubuntu 12.04'
today = datetime.date.today().strftime('%Y%m%d')

# TODO truncate first.

uuids = [str(uuid.uuid1()) for x in range(100000)]
oops_batcher = oops_cf.batch()
dayoops_batcher = dayoops_cf.batch()
for k in uuids:
    ident = hashlib.sha512(str(random.random())).hexdigest()
    data = {'SystemIdentifier' : ident,
            'DistroRelease' : 'Ubuntu 12.04'}
    oops_batcher = oops_batcher.insert(k, data)
    dayoops_batcher = dayoops_batcher.insert(today, {uuid.uuid1(): k})
oops_batcher.send()
dayoops_batcher.send()

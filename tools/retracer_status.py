#!/usr/bin/python

import pycassa
from pycassa.cassandra.ttypes import NotFoundException
import datetime
from time import sleep
from daisy import config

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                              config.cassandra_hosts, timeout=600,
                              credentials=creds)

retracestats_cf = pycassa.ColumnFamily(pool, 'RetraceStats')

def main():
    # check to see if the retracing counts are changing
    date = datetime.date.today().strftime('%Y%m%d')
    try:
        previous_stats = retracestats_cf.get(date)
    # this could happen at the start of the day
    except NotFoundException:
        print('status=retracing')
    sleep(180)
    current_stats = retracestats_cf.get(date)
    if previous_stats == current_stats:
        print('status=stopped')
    else:
        print('status=retracing')

if __name__ == '__main__':
    main()

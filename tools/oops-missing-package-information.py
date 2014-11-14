#!/usr/bin/python
import pycassa
import sys
from datetime import datetime, timedelta

from daisy import config

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                              config.cassandra_hosts, timeout=10,
                              credentials=creds)
oops_cf = pycassa.ColumnFamily(pool, 'OOPS')
old_date = datetime.today() - timedelta(days=7)
count = 0
for oops, oops_data in oops_cf.get_range(columns=['Date','Package'],
        row_count=1000):
    if count >= 10:
        sys.exit(0)
    date_str = oops_data.get('Date', '')
    try:
        date = datetime.strptime(date_str, '%a %b %d %H:%M:%S %Y')
    except ValueError:
        continue
    if date.date() >= old_date.date():
        continue
    pkg = oops_data.get('Package', '')
    if pkg:
        continue
    print("https://errors.ubuntu.com/oops/%s" % oops)
    count += 1

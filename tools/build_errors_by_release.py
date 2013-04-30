#!/usr/bin/python

import datetime
import pycassa
import uuid
from pycassa.cassandra.ttypes import NotFoundException
from daisy import config
from collections import Counter
import argparse
import sys

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                              config.cassandra_hosts, timeout=10,
                              max_retries=100, credentials=creds)

oops_cf = pycassa.ColumnFamily(pool, 'OOPS')
firsterror = None
errorsbyrelease = None
systems = None

columns = ['SystemIdentifier', 'DistroRelease']
kwargs = {
    'buffer_size': 1024 * 10,
    'include_timestamp': True,
    'columns': columns,
}

def main(verbose=False):
    count = 0
    for key, oops in oops_cf.get_range(**kwargs):
        if verbose:
            count += 1
            if count % 100000 == 0:
                print 'processed', count
                sys.stdout.flush()

        if Counter(columns) != Counter(oops.keys()):
            continue

        # Some bogus release names, like that of
        # 146104fadced68c9dedfd124427b7e05d62511b3c79743dd7b63465bb090f472
        # a6a5b34f32f8ac120ac47003f2a9f08030d368427cdf161cfa9ebad2ec8044bd
        release = oops['DistroRelease'][0].encode('utf8')
        if len(release) > 2048 or '\n' in release:
            # Bogus data.
            continue
        system_token = oops['SystemIdentifier'][0]

        if not release.startswith('Ubuntu '):
            continue

        occurred = oops['DistroRelease'][1] / 1000000
        occurred = datetime.datetime.fromtimestamp(occurred)
        occurred = occurred.replace(hour=0, minute=0, second=0, microsecond=0)

        first_error_date = None
        try:
            first_error_date = firsterror.get(release, columns=[system_token])
            first_error_date = first_error_date[system_token]
        except NotFoundException:
            pass

        if not first_error_date or first_error_date > occurred:
            firsterror.insert(release, {system_token: occurred})
            first_error_date = occurred

        oops_id = uuid.UUID(key)
        errorsbyrelease.insert((release, occurred), {oops_id: first_error_date})
        # We want to measure just the systems that have reported a
        # DistroRelease field and are running an official Ubuntu release.
        systems.insert((release, occurred), {system_token: ''})

    if verbose:
        print 'total processed', count

def parse_options():
    parser = argparse.ArgumentParser(
                description='Back-populate ErrorsByRelease and FirstError.')
    parser.add_argument('--write-hosts', nargs='+',
                        help='Cassandra host and IP (colon-separated) to write'
                        ' results to.')
    return parser.parse_args()

if __name__ == '__main__':
    options = parse_options()
    if options.write_hosts:
        write_pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                                            options.write_hosts, timeout=60,
                                            pool_size=15, max_retries=100,
                                            credentials=creds)
        firsterror = pycassa.ColumnFamily(write_pool, 'FirstError')
        errorsbyrelease = pycassa.ColumnFamily(write_pool, 'ErrorsByRelease')
        systems = pycassa.ColumnFamily(write_pool, 'SystemsForErrorsByRelease')

    main(True)
 

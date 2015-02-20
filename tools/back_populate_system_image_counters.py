#!/usr/bin/python

# need to backfill counters for devices from before 20150122

import sys
import pycassa
from pycassa.cassandra.ttypes import NotFoundException
from collections import defaultdict
from daisy import config
from daisy import utils

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                              config.cassandra_hosts, timeout=600,
                              credentials=creds)

dayoops_cf = pycassa.ColumnFamily(pool, 'DayOOPS')
oops_cf = pycassa.ColumnFamily(pool, 'OOPS')
counters_cf = pycassa.ColumnFamily(pool, 'Counters')

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print >>sys.stderr, "Usage: [%Y%m%d]"
        sys.exit(1)
    oopses = set()
    start = ''
    date = sys.argv[1]
    while True:
        try:
            buf = dayoops_cf.get(date, column_start=start, column_count=5000)
        except NotFoundException:
            break
        start = buf.keys()[-1]
        buf = buf.values()
        oopses.update(buf)
        if len(buf) < 1000:
            break
    #from ipdb import set_trace; set_trace()
    results = defaultdict(int)
    count = 0
    keys = []
    for oops in oopses:
        try:
            data = oops_cf.get(str(oops), columns=['SystemImageInfo',
                    'DistroRelease', 'Package', 'PackageArchitecture',
                    'Architecture', 'SystemIdentifier'])
            release = data.get('DistroRelease', '')
            if not release.startswith('Ubuntu '):
                continue
            # If its don't from a device we don't need to recount it
            if 'SystemImageInfo' not in data:
                continue
            # Don't add counters for devices under automated testing
            sys_id = data.get('SystemIdentifier', '')
            if sys_id.startswith('deadbeef'):
                continue
            pkg_arch = data.get('PackageArchitecture', '')
            if pkg_arch == 'all':
                arch = data.get('Architecture', '')
                pkg_arch = arch
            sii_dict = {}
            for line in data['SystemImageInfo'].splitlines():
                sii_dict[line.split(':')[0]] = ':'.join(line.split(':')[1:]).strip()
            channel = sii_dict.get('channel', '')
            device_name = sii_dict.get('device name', '')
            package, version = utils.split_package_and_version(data.get('Package', ''))
            if release and channel and package and version and pkg_arch:
                keys.append('%s:%s:%s:%s:%s' % (release, channel, package, version, pkg_arch))
                keys.append('%s:%s:%s:%s' % (release, channel, package, pkg_arch))
                keys.append('%s:%s:%s' % (release, channel, pkg_arch))
                keys.append('%s:%s:%s:%s' % (channel, package, version, pkg_arch))
                keys.append('%s:%s:%s' % (channel, package, pkg_arch))
                keys.append('%s:%s' % (channel, pkg_arch))
                if device_name:
                    keys.append('%s:%s:%s:%s:%s:%s' % (release, channel, device_name, package, version, pkg_arch))
                    keys.append('%s:%s:%s:%s:%s' % (release, channel, device_name, package, pkg_arch))
                    keys.append('%s:%s:%s:%s' % (release, channel, device_name, pkg_arch))
                    keys.append('%s:%s:%s:%s:%s' % (channel, device_name, package, version, pkg_arch))
                    keys.append('%s:%s:%s:%s' % (channel, device_name, package, pkg_arch))
                    keys.append('%s:%s:%s' % (channel, device_name, pkg_arch))
            if release and channel and package and version:
                keys.append('%s:%s:%s:%s' % (release, channel, package, version))
                keys.append('%s:%s:%s' % (release, channel, package))
                keys.append('%s:%s' % (release, channel))
            if channel and package and version:
                keys.append('%s:%s:%s' % (channel, package, version))
                keys.append('%s:%s' % (channel, package))
                keys.append('%s' % (channel))
            if release and channel and device_name and package and version:
                keys.append('%s:%s:%s:%s:%s' % (release, channel, device_name, package, version))
                keys.append('%s:%s:%s:%s' % (release, channel, device_name, package))
                keys.append('%s:%s:%s' % (release, channel, device_name))
                keys.append('%s:%s' % (release, channel))
            if channel and device_name and package and version:
                keys.append('%s:%s:%s:%s' % (channel, device_name, package, version))
                keys.append('%s:%s:%s' % (channel, device_name, package))
                keys.append('%s:%s' % (channel, device_name))
                keys.append('%s' % channel)
        except (NotFoundException):
            # Sometimes we didn't insert the full OOPS. I have no idea why.
            #print 'could not find', uuid
            continue
        for key in keys:
            results[key] += 1
        #count += 1
        #if count > 1000:
        #    break
    for result in results:
        k = 'oopses:%s' % result
        try:
            v = counters_cf.get(k, columns=[date])
            print k, date, 'already exists! Skipping.', v
        except NotFoundException:
            print 'adding', k, date, results[result]
            counters_cf.add(k, date, results[result])

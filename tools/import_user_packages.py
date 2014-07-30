#!/usr/bin/python
import pycassa
from daisy import launchpad
from daisy import config

creds = {'username': config.cassandra_username,
         'password': config.cassandra_password}
pool = pycassa.ConnectionPool(config.cassandra_keyspace,
                              config.cassandra_hosts, timeout=15,
                              credentials=creds)
userbinpkgs_cf = pycassa.ColumnFamily(pool, 'UserBinaryPackages')

def import_user_binary_packages(user):
    binary_packages = launchpad.get_subscribed_packages(user)
    for binary_package in binary_packages:
        #print("%s: %s" % (user, binary_package))
        userbinpkgs_cf.insert(user, {binary_package: ''})

if __name__ == '__main__':
    teams = ['ubuntu-x-swat', 'desktop-packages', 'ubuntu-server',
        'foundations-bugs', 'dx-packages', 'edubuntu-bugs', 'kubuntu-bugs',
        'lubuntu-packaging', 'xubuntu-bugs', 'ubuntu-security-bugs', 'laney',
        'kernel-packages', 'ubuntu-apps-bugs', 'ubuntu-phonedations-bugs',
        'ubuntu-sdk-bugs', 'ubuntu-webapps-bugs', 'ubuntuone-hackers',
        'unity-api-bugs', 'unity-ui-bugs']
    for team in teams:
        import_user_binary_packages(team)

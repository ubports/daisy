#!/usr/bin/python
import pycassa
from pycassa.cassandra.ttypes import NotFoundException
from daisy import launchpad

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass
if not configuration:
    import configuration

creds = {'username': configuration.cassandra_username,
         'password': configuration.cassandra_password}
pool = pycassa.ConnectionPool(configuration.cassandra_keyspace,
                              configuration.cassandra_hosts, timeout=15,
                              credentials=creds)
userbinpkgs_cf = pycassa.ColumnFamily(pool, 'UserBinaryPackages')

def import_user_binary_packages(user):
    binary_packages = launchpad.get_subscribed_packages(user)
    for binary_package in binary_packages:
        userbinpkgs_cf.insert(user, {binary_package: ''})

if __name__ == '__main__':
    teams = ['ubuntu-x-swat', 'desktop-packages', 'ubuntu-server',
        'foundations-bugs', 'dx-packages', 'edubuntu-bugs', 'kubuntu-bugs',
        'lubuntu-packaging', 'xubuntu-bugs', 'ubuntu-security']
    for team in teams:
        import_user_binary_packages(team)

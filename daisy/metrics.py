#!/usr/bin/python
from txstatsd.client import UdpStatsDClient
from txstatsd.metrics.metrics import Metrics

import pycassa

from daisy import config

METRICS = None
def get_metrics():
    global METRICS
    if METRICS is None:
        connection = UdpStatsDClient(host=config.statsd_host,
                                     port=config.statsd_port)
        connection.connect()
        METRICS = Metrics(connection=connection, namespace='whoopsie-daisy.daisy')
    return METRICS

class VerboseListener(pycassa.pool.PoolListener):
    def connection_checked_in(self, dic):
        print 'connection_checked_in', dic
    def connection_checked_out(self, dic):
        print 'connection_checked_out', dic
    def connection_created(self, dic):
        print 'connection_created', dic
    def connection_disposed(self, dic):
        print 'connection_disposed', dic
    def connection_failed(self, dic):
        print 'connection_failed', dic
    def connection_recycled(self, dic):
        print 'connection_recycled', dic
    def pool_at_max(self, dic):
        print 'pool_at_max', dic
    def pool_disposed(self, dic):
        print 'pool_disposed', dic
    def server_list_obtained(self, dic):
        print 'server_list_obtained', dic

class FailureListener(pycassa.pool.PoolListener):
    def connection_failed(self, dic):
        name = 'cassandra_connection_failures'
        get_metrics().increment(name)

# TODO: Specifying a separate namespace for the retracers.
def failure_wrapped_connection_pool():
    creds = {'username': config.cassandra_username,
             'password': config.cassandra_password}
    return pycassa.ConnectionPool(config.cassandra_keyspace,
                                  config.cassandra_hosts,
                                  listeners=[FailureListener()], timeout=30,
                                  # I have no idea why max_retries is
                                  # evaluating as 0 when not set, but here we
                                  # are, brute forcing this.
                                  max_retries=5, credentials=creds)

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

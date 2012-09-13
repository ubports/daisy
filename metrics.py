#!/usr/bin/python
from txstatsd.client import UdpStatsDClient
from txstatsd.metrics.metrics import Metrics

import pycassa

configuration = None
try:
    import local_config as configuration
except ImportError:
        pass
if not configuration:
    from daisy import configuration

METRICS = None
def get_metrics():
    global METRICS
    if METRICS is None:
        connection = UdpStatsDClient(host=configuration.statsd_host,
                                     port=configuration.statsd_port)
        connection.connect()
        METRICS = Metrics(connection=connection, namespace='whoopsie-daisy.daisy')
    return METRICS

class FailureListener(pycassa.pool.PoolListener):
    def connection_failed(self, dic):
        name = 'cassandra_connection_failures'
        get_metrics().increment(name)

# TODO: Specifying a separate namespace for the retracers.
def failure_wrapped_connection_pool():
    creds = {'username': configuration.cassandra_username,
             'password': configuration.cassandra_password}
    return pycassa.ConnectionPool(configuration.cassandra_keyspace,
                                  configuration.cassandra_hosts,
                                  listeners=[FailureListener()], timeout=30,
                                  # I have no idea why max_retries is
                                  # evaluating as 0 when not set, but here we
                                  # are, brute forcing this.
                                  max_retries=5, credentials=creds)

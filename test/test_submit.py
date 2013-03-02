#!/usr/bin/python

import unittest
import mock
import bson
import apport
from cStringIO import StringIO
from testtools import TestCase
from oopsrepository.testing.cassandra import TemporaryOOPSDB
import pycassa
import tempfile
import shutil
import os
import time
import uuid

from oopsrepository import oopses
from oopsrepository import schema as oopsschema
from oopsrepository import config as oopsconfig
# Attempting to import local_config is necessary as the modules under test may
# import it and we need to monkeypatch the configuration module in use.
configuration = None
try:
    import local_config as configuration
except ImportError:
    pass
if not configuration:
    from daisy import configuration
from daisy import submit
from daisy import schema
from daisy import wsgi

# SHA-512 of the system-uuid
sha512_system_uuid = ('/d78abb0542736865f94704521609c230dac03a2f369d043ac212d6'
                      '933b91410e06399e37f9c5cc88436a31737330c1c8eccb2c2f9f374'
                      'd62f716432a32d50fac')

class TestSubmission(TestCase):
    def setUp(self):
        super(TestSubmission, self).setUp()
        self.start_response = mock.Mock()

        # Set up daisy schema.
        os.environ['OOPS_HOST'] = configuration.cassandra_hosts[0]
        self.keyspace = self.useFixture(TemporaryOOPSDB()).keyspace
        os.environ['OOPS_KEYSPACE'] = self.keyspace
        configuration.cassandra_keyspace = self.keyspace
        self.creds = {'username': configuration.cassandra_username,
                      'password': configuration.cassandra_password}
        schema.create()

        # Set up oopsrepository schema.
        oops_config = oopsconfig.get_config()
        oops_config['username'] = configuration.cassandra_username
        oops_config['password'] = configuration.cassandra_password
        oopsschema.create(oops_config)

        # Clear singletons.
        wsgi._pool = None
        oopses._connection_pool = None
        submit.oops_config = oops_config

class TestCrashSubmission(TestSubmission):

    def test_bogus_submission(self):
        environ = {'PATH_INFO': '/', 'wsgi.input': StringIO('')}
        wsgi.app(environ, self.start_response)
        self.assertEqual(self.start_response.call_args[0][0], '400 Bad Request')

    def test_python_submission(self):
        '''Ensure that a Python crash is accepted, bucketed, and that the
        retracing ColumnFamilies remain untouched.'''

        report = apport.Report()
        report['ProblemType'] = 'Crash'
        report['InterpreterPath'] = '/usr/bin/python'
        report['ExecutablePath'] = '/usr/bin/foo'
        report['DistroRelease'] = 'Ubuntu 12.04'
        report['Package'] = 'ubiquity 2.34'
        report['Traceback'] = ('Traceback (most recent call last):\n'
                               '  File "/usr/bin/foo", line 1, in <module>\n'
                               '    sys.exit(1)')
        report_bson = bson.BSON.encode(report.data)
        report_io = StringIO(report_bson)
        environ = { 'CONTENT_TYPE' : 'application/octet-stream',
                    'PATH_INFO' : sha512_system_uuid,
                    'wsgi.input' : report_io }

        wsgi.app(environ, self.start_response)
        self.assertEqual(self.start_response.call_args[0][0], '200 OK')

        pool = pycassa.ConnectionPool(self.keyspace, configuration.cassandra_hosts,
                                      credentials=self.creds)
        oops_cf = pycassa.ColumnFamily(pool, 'OOPS')
        bucket_cf = pycassa.ColumnFamily(pool, 'Bucket')
        # Ensure the crash was bucketed:
        oops_id = oops_cf.get_range().next()[0]
        crash_signature = '/usr/bin/foo:    sys.exit(1):/usr/bin/foo@1'
        self.assertEqual(uuid.uuid1(oops_id), bucket_cf.get(crash_signature).keys()[0])

        # A Python crash shouldn't touch the retracing CFs:
        for fam in ('AwaitingRetrace', 'Stacktrace', 'Indexes'):
            cf = pycassa.ColumnFamily(pool, fam)
            self.assertEqual([x for x in cf.get_range()], [])
        cf = pycassa.ColumnFamily(pool, 'DayBucketsCount')
        counts = [x for x in cf.get_range()]
        day_key = time.strftime('%Y%m%d', time.gmtime())
        resolutions = (day_key, day_key[:4], day_key[:6])
        release = report['DistroRelease']
        keys = []
        for resolution in resolutions:
            keys.append('%s:%s' % (release, resolution))
        for resolution in resolutions:
            keys.append('%s:ubiquity:%s' % (release, resolution))
        for resolution in resolutions:
            keys.append('%s:ubiquity:2.34:%s' % (release, resolution))
        for resolution in resolutions:
            keys.append('ubiquity:2.34:%s' % resolution)
        'ubiquity:2.34'

        for key in keys:
            found = False
            for count in counts:
                if count[0] == key:
                    found = True
            self.assertTrue(found, 'Could not find %s' % key)
            for count in counts:
                self.assertEqual(count[1].values(), [1])

class TestBinarySubmission(TestCrashSubmission):
    def setUp(self):
        super(TestBinarySubmission, self).setUp()
        self.stack_addr_sig = (
            '/usr/bin/foo:11:x86_64/lib/x86_64-linux-gnu/libc-2.15.so+e4d93:'
            '/usr/bin/foo+1e071')
        report = apport.Report()
        report['ProblemType'] = 'Crash'
        report['StacktraceAddressSignature'] = self.stack_addr_sig
        report['ExecutablePath'] = '/usr/bin/foo'
        report['Package'] = 'whoopsie 1.2.3'
        report['DistroRelease'] = 'Ubuntu 12.04'
        report_bson = bson.BSON.encode(report.data)
        report_io = StringIO(report_bson)
        self.environ = { 'CONTENT_TYPE' : 'application/octet-stream',
                         'PATH_INFO' : sha512_system_uuid,
                         'wsgi.input' : report_io }

    def test_binary_submission_not_retraced(self):
        '''If a binary crash has been submitted that we do not have a core file
        for, either already retraced or awaiting to be retraced.'''

        resp = wsgi.app(self.environ, self.start_response)[0]
        self.assertEqual(self.start_response.call_args[0][0], '200 OK')
        # We should get back a request for the core file:
        self.assertTrue(resp.endswith(' CORE'))

        # It should end up in the AwaitingRetrace CF queue.
        pool = pycassa.ConnectionPool(self.keyspace, configuration.cassandra_hosts,
                                      credentials=self.creds)
        awaiting_retrace_cf = pycassa.ColumnFamily(pool, 'AwaitingRetrace')
        oops_cf = pycassa.ColumnFamily(pool, 'OOPS')
        oops_id = oops_cf.get_range().next()[0]
        self.assertEqual(
            awaiting_retrace_cf.get(self.stack_addr_sig).keys()[0], oops_id)

    def test_binary_submission_retrace_queued(self):
        '''If a binary crash has been submitted that we do have a core file
        for, but it has not been retraced yet.'''
        # Lets pretend we've seen the stacktrace address signature before, and
        # have received a core file for it, but have not finished retracing it:
        pool = pycassa.ConnectionPool(self.keyspace, configuration.cassandra_hosts,
                                      credentials=self.creds)
        awaiting_retrace_cf = pycassa.ColumnFamily(pool, 'AwaitingRetrace')
        oops_cf = pycassa.ColumnFamily(pool, 'OOPS')
        indexes_cf = pycassa.ColumnFamily(pool, 'Indexes')
        indexes_cf.insert('retracing', {self.stack_addr_sig : ''})

        resp = wsgi.app(self.environ, self.start_response)[0]
        self.assertEqual(self.start_response.call_args[0][0], '200 OK')
        # We should not get back a request for the core file:
        self.assertEqual(resp, '')
        # Ensure the crash was bucketed and added to the AwaitingRetrace CF
        # queue:
        oops_id = oops_cf.get_range().next()[0]
        self.assertEqual(
            awaiting_retrace_cf.get(self.stack_addr_sig).keys()[0], oops_id)

    def test_binary_submission_already_retraced(self):
        '''If a binary crash has been submitted that we have a fully-retraced
        core file for.'''
        pool = pycassa.ConnectionPool(self.keyspace, configuration.cassandra_hosts,
                                      credentials=self.creds)
        indexes_cf = pycassa.ColumnFamily(pool, 'Indexes')
        bucket_cf = pycassa.ColumnFamily(pool, 'Bucket')
        oops_cf = pycassa.ColumnFamily(pool, 'OOPS')

        indexes_cf.insert('crash_signature_for_stacktrace_address_signature',
                          {self.stack_addr_sig : 'fake-crash-signature'})

        resp = wsgi.app(self.environ, self.start_response)[0]
        self.assertEqual(self.start_response.call_args[0][0], '200 OK')
        # We should not get back a request for the core file:
        self.assertEqual(resp, '')
        
        # Make sure 'foo' ends up in the bucket.
        oops_id = oops_cf.get_range().next()[0]
        bucket_contents = bucket_cf.get('fake-crash-signature').keys()
        self.assertEqual(bucket_contents, [uuid.uuid1(oops_id)])

class TestCoreSubmission(TestSubmission):
    def setUp(self):
        super(TestCoreSubmission, self).setUp()
        self.conn_mock = mock.MagicMock()
        # TODO in the future, we may want to just set up a local Rabbit MQ,
        # like we do with Cassandra.
        amqp_connection = mock.patch('amqplib.client_0_8.Connection', self.conn_mock)
        amqp_connection.start()
        self.msg_mock = mock.MagicMock()
        amqp_msg = mock.patch('amqplib.client_0_8.Message', self.msg_mock)
        amqp_msg.start()
        self.addCleanup(amqp_msg.stop)
        self.addCleanup(amqp_connection.stop)

    def test_core_submission(self):
        data = 'I am an ELF binary. No, really.'
        core_io = StringIO(data)
        uuid = '12345678-1234-5678-1234-567812345678'
        environ = {'QUERY_STRING' : 'uuid=%s&arch=amd64' % uuid,
                   'CONTENT_TYPE' : 'application/octet-stream',
                   'wsgi.input' : core_io,
                   'PATH_INFO': '/%s/submit-core/amd64' % uuid}
        stack_addr_sig = (
            '/usr/bin/foo:11:x86_64/lib/x86_64-linux-gnu/libc-2.15.so+e4d93:'
            '/usr/bin/foo+1e071')
        path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, path)
        configuration.san_path = path
        pool = pycassa.ConnectionPool(self.keyspace, configuration.cassandra_hosts,
                                      credentials=self.creds)
        oops_cf = pycassa.ColumnFamily(pool, 'OOPS')
        oops_cf.insert(uuid, {'StacktraceAddressSignature' : stack_addr_sig})

        wsgi.app(environ, self.start_response)
        self.assertEqual(self.start_response.call_args[0][0], '200 OK')

        # Did we actually write the core file to disk?
        with open(os.path.join(path, uuid)) as fp:
            contents = fp.read()
        self.assertEqual(contents, data)

        # Did we put the crash on the retracing queue?
        channel = self.conn_mock.return_value.channel
        basic_publish_call = channel.return_value.basic_publish.call_args
        kwargs = basic_publish_call[1]
        self.assertEqual(kwargs['routing_key'], 'retrace_amd64')
        self.assertEqual(kwargs['exchange'], '')
        self.assertEqual(self.msg_mock.call_args[0][0],
                         os.path.join(path, uuid))

        # did we mark this as retracing in Cassandra?
        indexes_cf = pycassa.ColumnFamily(pool, 'Indexes')
        indexes_cf.get('retracing', [stack_addr_sig])

if __name__ == '__main__':
    unittest.main()

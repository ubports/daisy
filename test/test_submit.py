#!/usr/bin/python

import unittest
import mock
import bson
import apport
from cStringIO import StringIO
from testtools import TestCase
from oopsrepository.testing.cassandra import TemporaryOOPSDB
import imp
import pycassa
import configuration
import schema

class TestSubmission(TestCase):
    def setUp(self):
        super(TestSubmission, self).setUp()
        # We need to set the configuration before loading the WSGI application.
        self.keyspace = self.useFixture(TemporaryOOPSDB()).keyspace
        configuration.cassandra_keyspace = self.keyspace
        configuration.cassandra_host = 'localhost:9160'
        schema.create()

        self.submit = imp.load_source('submit', 'backend/submit.wsgi')

    def test_bogus_submission(self):
        environ = {}
        start_response = mock.Mock()
        self.submit.application(environ, start_response)
        self.assertEqual(start_response.call_args[0][0], '400 Bad Request')

    def test_python_submission(self):
        '''Ensure that a Python crash is accepted, bucketed, and that the
        retracing ColumnFamilies remain untouched.'''
        environ = {}
        start_response = mock.Mock()

        report = apport.Report()
        report['ProblemType'] = 'Crash'
        report['InterpreterPath'] = '/usr/bin/python'
        report['ExecutablePath'] = '/usr/bin/foo'
        report['Traceback'] = ('Traceback (most recent call last):\n'
                               '  File "/usr/bin/foo", line 1, in <module>\n'
                               '    sys.exit(1)')
        report_bson = bson.BSON.encode(report.data)
        report_io = StringIO(report_bson)
        environ = { 'CONTENT_TYPE' : 'application/octet-stream',
                    # SHA-512 of the system-uuid
                    'PATH_INFO' : '/d78abb0542736865f94704521609c230dac03a2f3'
                                  '69d043ac212d6933b91410e06399e37f9c5cc88436'
                                  'a31737330c1c8eccb2c2f9f374d62f716432a32d50'
                                  'fac',
                    'wsgi.input' : report_io }

        self.submit.application(environ, start_response)
        self.assertEqual(start_response.call_args[0][0], '200 OK')

        pool = pycassa.ConnectionPool(self.keyspace, ['localhost:9160'])
        oops_cf = pycassa.ColumnFamily(pool, 'OOPS')
        bucket_cf = pycassa.ColumnFamily(pool, 'Buckets')
        # Ensure the crash was bucketed:
        oops_id = oops_cf.get_range().next()[0]
        crash_signature = '/usr/bin/foo:    sys.exit(1):/usr/bin/foo@1'
        self.assertEqual(oops_id, bucket_cf.get(crash_signature).keys()[0])

        # A Python crash shouldn't touch the retracing CFs:
        for fam in ('AwaitingRetrace', 'Stacktrace', 'Indexes'):
            cf = pycassa.ColumnFamily(pool, fam)
            self.assertEqual([x for x in cf.get_range()], [])


if __name__ == '__main__':
    unittest.main()

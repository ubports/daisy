# -*- coding: utf8 -*-
import unittest
import mock
from testtools import TestCase
from oopsrepository.testing.cassandra import TemporaryOOPSDB
from oopsrepository import schema as oopsschema
from oopsrepository import config as oopsconfig
from daisy import schema
from daisy import retracer
from daisy import config
import tempfile
import os
import shutil
import time
import pycassa
from pycassa.types import IntegerType, FloatType

class TestSubmission(TestCase):
    def setUp(self):
        super(TestSubmission, self).setUp()
        # We need to set the config before importing.
        os.environ['OOPS_HOST'] = config.cassandra_hosts[0]
        self.keyspace = self.useFixture(TemporaryOOPSDB()).keyspace
        os.environ['OOPS_KEYSPACE'] = self.keyspace
        creds = {'username': config.cassandra_username,
                 'password': config.cassandra_password}
        self.pool = pycassa.ConnectionPool(self.keyspace,
                                           config.cassandra_hosts,
                                           credentials=creds)
        config.cassandra_keyspace = self.keyspace
        schema.create()
        oops_config = oopsconfig.get_config()
        oops_config['username'] = config.cassandra_username
        oops_config['password'] = config.cassandra_password
        oopsschema.create(oops_config)
        self.temp = tempfile.mkdtemp()
        config_dir = os.path.join(self.temp, 'config')
        sandbox_dir = os.path.join(self.temp, 'sandbox')
        os.makedirs(config_dir)
        os.makedirs(sandbox_dir)
        self.architecture = 'amd64'
        # Don't depend on apport-retrace being installed.
        with mock.patch('daisy.retracer.Popen') as popen:
            popen.return_value.returncode = 0
            popen.return_value.communicate.return_value = ['/bin/false']
            self.retracer = retracer.Retracer(config_dir, sandbox_dir,
                                              self.architecture, False, False)

    def tearDown(self):
        super(TestSubmission, self).tearDown()
        shutil.rmtree(self.temp)

    def test_update_retrace_stats(self):
        retrace_stats_fam = pycassa.ColumnFamily(self.pool, 'RetraceStats')
        indexes_fam = pycassa.ColumnFamily(self.pool, 'Indexes')
        release = 'Ubuntu 12.04'
        day_key = time.strftime('%Y%m%d', time.gmtime())

        self.retracer.update_retrace_stats(release, day_key, 30.5, True)
        result = retrace_stats_fam.get(day_key)
        self.assertEqual(result['Ubuntu 12.04:success'], 1)
        mean_key = '%s:%s:%s' % (day_key, release, self.architecture)
        counter_key = '%s:count' % mean_key
        indexes_fam.column_validators = {mean_key : FloatType(),
                                         counter_key : IntegerType()}
        result = indexes_fam.get('mean_retracing_time')
        self.assertEqual(result[mean_key], 30.5)
        self.assertEqual(result[counter_key], 1)

        self.retracer.update_retrace_stats(release, day_key, 30.5, True)
        result = indexes_fam.get('mean_retracing_time')
        self.assertEqual(result[mean_key], 30.5)
        self.assertEqual(result[counter_key], 2)

    def test_chunked_insert(self):
        # UnicodeEncodeError: 'ascii' codec can't encode character u'\xe9' in
        # position 487: ordinal not in range(128)
        stack_fam = pycassa.ColumnFamily(self.pool, 'Stacktrace')
        stack_fam.default_validation_class = pycassa.types.UTF8Type()

        # Non-chunked version.
        data = {'Package': 'apport', 'ProblemType': 'Crash'}
        retracer.chunked_insert(stack_fam, 'foo', data) 
        results = stack_fam.get_range().next()
        self.assertEqual(results[0], 'foo')
        self.assertEqual(results[1]['Package'], 'apport')
        self.assertEqual(results[1]['ProblemType'], 'Crash')

        # Chunked.
        stack_fam.truncate()
        data['Big'] = 'a' * (1024 * 1024 * 4 + 1)
        retracer.chunked_insert(stack_fam, 'foo', data)
        results = stack_fam.get_range().next()
        self.assertEqual(results[0], 'foo')
        self.assertEqual(results[1]['Package'], 'apport')
        self.assertEqual(results[1]['ProblemType'], 'Crash')
        self.assertEqual(results[1]['Big'], 'a' * 1024 * 1024 * 4)
        self.assertEqual(results[1]['Big-1'], 'a')

        # Unicode. As generated in callback(), oops_fam.get()
        stack_fam.truncate()
        data = {u'☃'.encode('utf8'): u'☕'.encode('utf8')}
        retracer.chunked_insert(stack_fam, 'foo', data)
        results = stack_fam.get_range().next()

if __name__ == '__main__':
    unittest.main()

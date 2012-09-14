#!/usr/bin/python
# -*- coding: utf-8 -*-
# 
# Copyright © 2011-2012 Canonical Ltd.
# Author: Evan Dandrea <evan.dandrea@canonical.com>
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero Public License as published by
# the Free Software Foundation; version 3 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero Public License for more details.
# 
# You should have received a copy of the GNU Affero Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import uuid
import bson

import os
from oopsrepository import config, oopses
import pycassa
from pycassa.cassandra.ttypes import NotFoundException

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass
if not configuration:
    import configuration
import apport
import utils
import metrics

os.environ['OOPS_KEYSPACE'] = configuration.cassandra_keyspace
oops_config = config.get_config()
oops_config['host'] = configuration.cassandra_hosts
oops_config['username'] = configuration.cassandra_username
oops_config['password'] = configuration.cassandra_password

# Cassandra connections. These may move into oopsrepository in the future.
pool = metrics.failure_wrapped_connection_pool()
indexes_fam = pycassa.ColumnFamily(pool, 'Indexes')
awaiting_retrace_fam = pycassa.ColumnFamily(pool, 'AwaitingRetrace')

content_type = 'CONTENT_TYPE'
ostream = 'application/octet-stream'

def ok_response(start_response, data=''):
    if data:
        start_response('200 OK', [('Content-type', 'text/plain')])
    else:
        start_response('200 OK', [])
    return [data]

def bad_request_response(start_response, text=''):
    start_response('400 Bad Request', [])
    return [text]

# TODO refactor into a class
def wsgi_handler(environ, start_response):
    global oops_config
    global pool, indexes_fam, awaiting_retrace_fam

    if not environ.has_key(content_type) or environ[content_type] != ostream:
        return bad_request_response(start_response, 'Incorrect Content-Type.')

    user_token = None
    # / + 128 character system UUID
    if len(environ['PATH_INFO']) == 129:
        user_token = environ['PATH_INFO'][1:]

    oops_id = str(uuid.uuid1())
    try:
        data = environ['wsgi.input'].read()
    except IOError, e:
        if e.message == 'request data read error':
            # The client disconnected while sending the report.
            return bad_request_response(start_response, 'Connection dropped.')
        else:
            raise
    try:
        data = bson.BSON(data).decode()
    except bson.errors.InvalidBSON:
        return bad_request_response(start_response, 'Invalid BSON.')
    # Keep a reference to the decoded report data. If we crash, we'll
    # potentially attach it to the OOPS report.
    environ['wsgi.input.decoded'] = data

    release = data.get('DistroRelease', '')
    package = data.get('Package', '')
    package, version = utils.split_package_and_version(package)
    fields = utils.get_fields_for_bucket_counters(release, package, version)
    oopses.insert_dict(oops_config, oops_id, data, user_token, fields)

    if 'InterpreterPath' in data and not 'StacktraceAddressSignature' in data:
        # Python crashes can be immediately bucketed.
        report = apport.Report()
        for key in ('ExecutablePath', 'Traceback', 'ProblemType'):
            try:
                report[key.encode('UTF-8')] = data[key].encode('UTF-8')
            except KeyError:
                return bad_request_response(start_response,
                    'Missing keys in interpreted report.')
        crash_signature = report.crash_signature()
        if crash_signature:
            utils.bucket(oops_config, oops_id, crash_signature, data)
            return ok_response(start_response)
        else:
            return bad_request_response(start_response,
                'Could not generate crash signature for interpreted report.')

    addr_sig = data.get('StacktraceAddressSignature', None)
    if not addr_sig:
        # We received BSON data with unexpected keys.
        return bad_request_response(start_response,
            'No StacktraceAddressSignature found in report.')

    # Binary
    output = ''
    crash_sig = None
    try:
        crash_sig = indexes_fam.get(
            'crash_signature_for_stacktrace_address_signature', [addr_sig])
        crash_sig = crash_sig.values()[0]
    except (NotFoundException, KeyError):
        pass
    if crash_sig:
        # We have already retraced for this address signature, so this crash
        # can be immediately bucketed.
        utils.bucket(oops_config, oops_id, crash_sig, data)
    else:
        # Are we already waiting for this stacktrace address signature to be
        # retraced?
        waiting = True
        try:
            indexes_fam.get('retracing', [addr_sig])
        except NotFoundException:
            waiting = False

        if not waiting and release.startswith('Ubuntu '):
            # We do not have a core file in the queue, so ask for one. Do
            # not assume we're going to get one, so also add this ID the
            # the AwaitingRetrace CF queue as well.

            # We don't ask derivatives for core dumps. We could technically
            # check to make sure the Packages and Dependencies fields do not
            # have '[origin:' lines; however, apport-retrace looks for
            # configuration data in a directory named by the DistroRelease, so
            # these would always fail regardless.
            output = '%s CORE' % oops_id

        awaiting_retrace_fam.insert(addr_sig, {oops_id : ''})
            
    return ok_response(start_response, output)

application = utils.wrap_in_oops_wsgi(wsgi_handler,
                                      configuration.oops_repository,
                                      'daisy.ubuntu.com')

#!/usr/bin/python
# -*- coding: utf-8 -*-
# 
# Copyright © 2011 Canonical Ltd.
# Author: Evan Dandrea <evan.dandrea@canonical.com>
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import uuid
import time
import bson

import os
from oopsrepository import config, oopses
import configuration

# Remember, oopsrepository is merely the API layer over Cassandra.  The rest
# lives here. So in the future, we'll call oopsrepository.insert().

content_type = 'CONTENT_TYPE'
ostream = 'application/octet-stream'
response_headers = [('Content-type', 'text/plain')]

os.environ['OOPS_KEYSPACE'] = configuration.cassandra_keyspace
oops_config = config.get_config()
oops_config['host'] = [configuration.cassandra_host]

def application(environ, start_response):
    global oops_config
    data = None
    if environ.has_key(content_type) and environ[content_type] == ostream:
        data = environ['wsgi.input'].read()
        user_token = None
        # / + 128 character system UUID
        if len(environ['PATH_INFO']) == 129:
            user_token = environ['PATH_INFO'][1:]
        row_key = str(uuid.uuid1())
        try:
            key = oopses.insert_bson(oops_config, row_key, data, user_token)
        except bson.errors.InvalidBSON:
            start_response('400 Bad Request', [])
            return []

        # If this is a Python traceback, there wont be a core file to retrace.
        if 'StacktraceAddressSignature' in data or 'InterpreterPath' in data:
            output = row_key
        else:
            output = '%s CORE' % row_key
            
    start_response('200 OK', response_headers)
    return [output]

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

# The Cassandra keyspace to use.
cassandra_keyspace = 'crashdb'

# The addresses of the Cassandra database nodes.
cassandra_hosts = ['127.0.0.1:9160']

# The AMQP host to receive messages from.
amqp_host = '127.0.0.1'

# The path to the SAN for storing core dumps.
san_path = '/srv/cores'

# The path to store OOPS reports in for http://errors.ubuntu.com.
oops_repository = '/srv/local-oopses-whoopsie'

# The host and port of the txstatsd server.
statsd_host = 'localhost'

statsd_port = 8125

# Use Launchpad staging instead of production.
lp_use_staging = False

# Directory for httplib2's request cache.
http_cache_dir = '/tmp/errors.ubuntu.com-httplib2-cache'

# Database configuration for the Errors Django application. This database is
# used to store OpenID login information.
django_databases = {
    'default' : {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'django_db',
        'USER': 'django_login',
        'PASSWORD': '',
        'HOST': 'localhost',
        'PORT': '5432',
        }
}

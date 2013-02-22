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

# The username to connect with.
cassandra_username = ''

# The password to use.
cassandra_password = ''

# The AMQP host to receive messages from.
amqp_host = '127.0.0.1'

# The AMQP username.
amqp_username = ''

# The AMQP username.
amqp_password = ''

# The path to the SAN for storing core dumps.
san_path = '/srv/cores'

# The path to store OOPS reports in for http://errors.ubuntu.com.
oops_repository = '/srv/local-oopses-whoopsie'

# The host and port of the txstatsd server.
statsd_host = 'localhost'

statsd_port = 8125

# Use Launchpad staging instead of production.
lp_use_staging = False

# Launchpad OAuth tokens.
# See https://wiki.ubuntu.com/ErrorTracker/Contributing/Errors
lp_oauth_token=''
lp_oauth_secret=''

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

# S3 configuration. Only set these if you're using S3 for storing the core
# files.
aws_access_key = ''
aws_secret_key = ''
ec2_host = ''

# The bucket to place core files in when using S3.
ec2_bucket = ''

# Swift configuration. Only set these if you're using Swift for storing the
# core files.
os_auth_url = ''
os_username = ''
os_password = ''
os_tenant_name = ''
os_region_name = ''

# The bucket to place core files in when using Swift.
swift_bucket = ''

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

# The Cassandra keyspace to use.
cassandra_keyspace = 'crashdb'

# The address of the Cassandra database.
cassandra_host = '127.0.0.1:9160'

# The AMQP host to receive messages from.
amqp_host = '127.0.0.1'

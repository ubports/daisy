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

from pycassa.system_manager import (
    SystemManager,
    UTF8_TYPE,
    LONG_TYPE,
    )

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass
if not configuration:
    import configuration
from oopsrepository.cassandra import workaround_1779
from pycassa.types import CounterColumnType

def create():
    keyspace = configuration.cassandra_keyspace
    creds = {'username': configuration.cassandra_username,
             'password': configuration.cassandra_password}
    mgr = SystemManager(configuration.cassandra_hosts[0], credentials=creds)
    cfs = mgr.get_keyspace_column_families(keyspace).keys()
    try:
        if 'Indexes' not in cfs:
            workaround_1779(mgr.create_column_family, keyspace, 'Indexes',
                comparator_type=UTF8_TYPE)
        if 'Stacktrace' not in cfs:
            workaround_1779(mgr.create_column_family, keyspace, 'Stacktrace',
                comparator_type=UTF8_TYPE,
                default_validation_class=UTF8_TYPE)
        if 'AwaitingRetrace' not in cfs:
            workaround_1779(mgr.create_column_family, keyspace, 'AwaitingRetrace',
                comparator_type=UTF8_TYPE,
                default_validation_class=UTF8_TYPE)
        if 'RetraceStats' not in cfs:
            workaround_1779(mgr.create_column_family, keyspace, 'RetraceStats',
                comparator_type=UTF8_TYPE,
                default_validation_class=CounterColumnType())
        if 'UniqueUsers90Days' not in cfs:
            workaround_1779(mgr.create_column_family, keyspace, 'UniqueUsers90Days',
                comparator_type=UTF8_TYPE,
                key_validation_class=UTF8_TYPE,
                default_validation_class=LONG_TYPE)
    finally:
        mgr.close()

if __name__ == '__main__':
    create()

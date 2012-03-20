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
    )

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass
if not configuration:
    import configuration
from oopsrepository.cassandra import workaround_1779

def create():
    keyspace = configuration.cassandra_keyspace
    mgr = SystemManager()
    try:
        workaround_1779(mgr.create_column_family, keyspace, 'Indexes',
            comparator_type=UTF8_TYPE)
        workaround_1779(mgr.create_column_family, keyspace, 'Stacktrace',
            comparator_type=UTF8_TYPE)
        workaround_1779(mgr.create_column_family, keyspace, 'AwaitingRetrace',
            comparator_type=UTF8_TYPE)
    finally:
        mgr.close()

if __name__ == '__main__':
    create()

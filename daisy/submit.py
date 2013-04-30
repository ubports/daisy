#!/usr/bin/python
# -*- coding: utf-8 -*-
# 
# Copyright © 2011-2013 Canonical Ltd.
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

from oopsrepository import config as oopsconfig
from oopsrepository import oopses
import pycassa
from pycassa.cassandra.ttypes import NotFoundException

from daisy import config
import apport
from daisy import utils
import time
import os

os.environ['OOPS_KEYSPACE'] = config.cassandra_keyspace
oops_config = oopsconfig.get_config()
oops_config['host'] = config.cassandra_hosts
oops_config['username'] = config.cassandra_username
oops_config['password'] = config.cassandra_password


def update_release_pkg_counter(counters_fam, release, src_package, date):
    counters_fam.insert('%s:%s' % (release, src_package), {date: 1})

def create_report_from_bson(data):
    report = apport.Report()
    for key in data:
        report[key.encode('UTF-8')] = data[key].encode('UTF-8')
    return report

def try_to_repair_sas(data):
    '''Try to repair the StacktraceAddressSignature, if this is a binary
       crash.'''

    if 'StacktraceTop' in data and 'Signal' in data:
        if not 'StacktraceAddressSignature' in data:
            report = create_report_from_bson(data)
            sas = report.crash_signature_addresses()
            if sas:
                data['StacktraceAddressSignature'] = sas

def submit(_pool, environ, system_token):
    counters_fam = pycassa.ColumnFamily(_pool, 'Counters',
                                        retry_counter_mutations=True)

    try:
        data = environ['wsgi.input'].read()
    except IOError as e:
        if e.message == 'request data read error':
            # The client disconnected while sending the report.
            return (False, 'Connection dropped.')
        else:
            raise
    try:
        data = bson.BSON(data).decode()
    except bson.errors.InvalidBSON:
        return (False, 'Invalid BSON.')

    # Keep a reference to the decoded report data. If we crash, we'll
    # potentially attach it to the OOPS report.
    environ['wsgi.input.decoded'] = data

    oops_id = str(uuid.uuid1())
    day_key = time.strftime('%Y%m%d', time.gmtime())

    if 'KernelCrash' in data or 'VmCore' in data:
        # We do not process these yet, but we keep track of how many reports
        # we're receiving to determine when it's worth solving.
        counters_fam.add('KernelCrash', day_key)
        return (False, 'Kernel crashes are not handled yet.')

    # Write the SHA512 hash of the system UUID in with the report.
    if system_token:
        data['SystemIdentifier'] = system_token

    release = data.get('DistroRelease', '')
    package = data.get('Package', '')
    src_package = data.get('SourcePackage', '')
    problem_type = data.get('ProblemType', '')
    third_party = False
    if '[origin:' in package:
        third_party = True

    package, version = utils.split_package_and_version(package)
    src_package, src_version = utils.split_package_and_version(src_package)
    fields = utils.get_fields_for_bucket_counters(problem_type, release, package, version)

    if not third_party and problem_type == 'Crash':
        update_release_pkg_counter(counters_fam, release, src_package, day_key)

    try_to_repair_sas(data)
    oopses.insert_dict(oops_config, oops_id, data, system_token, fields)

    success, output = bucket(_pool, oops_config, oops_id, data, day_key)
    return (success, output)

def bucket(_pool, oops_config, oops_id, data, day_key):
    '''Bucket oops_id.
       If the report was malformed, return (False, failure_msg)
       If a core file is to be requested, return (True, 'UUID CORE')
       If no further action is needed, return (True, '')
    '''

    indexes_fam = pycassa.ColumnFamily(_pool, 'Indexes')

    # General bucketing
    report = create_report_from_bson(data)
    crash_signature = report.crash_signature()
    crash_signature = utils.format_crash_signature(crash_signature)
    if crash_signature:
        utils.bucket(oops_config, oops_id, crash_signature, data)
        return (True, '')

    # Binary
    if 'StacktraceTop' in data and 'Signal' in data:
        output = ''
        addr_sig = data.get('StacktraceAddressSignature', None)
        if not addr_sig:
            counters_fam = pycassa.ColumnFamily(_pool, 'Counters',
                                                retry_counter_mutations=True)
            counters_fam.add('MissingSAS', day_key)
            # We received BSON data with unexpected keys.
            return (False, 'No StacktraceAddressSignature found in report.')

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

            release = data.get('DistroRelease', '')
            if not waiting and utils.retraceable_release(release):
                # We do not have a core file in the queue, so ask for one. Do
                # not assume we're going to get one, so also add this ID the
                # the AwaitingRetrace CF queue as well.

                # We don't ask derivatives for core dumps. We could technically
                # check to make sure the Packages and Dependencies fields do not
                # have '[origin:' lines; however, apport-retrace looks for
                # configuration data in a directory named by the DistroRelease, so
                # these would always fail regardless.
                output = '%s CORE' % oops_id

            awaiting_retrace_fam = pycassa.ColumnFamily(_pool, 'AwaitingRetrace')
            awaiting_retrace_fam.insert(addr_sig, {oops_id : ''})
        return (True, output)

    # Could not bucket
    could_not_bucket_fam = pycassa.ColumnFamily(_pool, 'CouldNotBucket')
    could_not_bucket_fam.insert(day_key, {uuid.UUID(oops_id): ''})
    return (True, '')

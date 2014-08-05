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
from pycassa.cassandra.ttypes import NotFoundException, InvalidRequestException

from daisy import config
import apport
from daisy import utils
from daisy.metrics import get_metrics
from datetime import datetime
import os
import socket
import sys
import time

os.environ['OOPS_KEYSPACE'] = config.cassandra_keyspace
oops_config = oopsconfig.get_config()
oops_config['host'] = config.cassandra_hosts
oops_config['username'] = config.cassandra_username
oops_config['password'] = config.cassandra_password
oops_config['pool_size'] = config.cassandra_pool_size
oops_config['max_overflow'] = config.cassandra_max_overflow

metrics = get_metrics('daisy.%s' % socket.gethostname())


def update_release_pkg_counter(counters_fam, release, src_package, date):
    counters_fam.insert('%s:%s' % (release, src_package), {date: 1})

def update_release_pkg_version_counter(counters_fam, release, src_package, src_version, date):
    counters_fam.insert('%s:%s:%s' % (release, src_package, src_version), {date: 1})

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
            metrics.meter('repair.tried_sas')
            report = create_report_from_bson(data)
            sas = report.crash_signature_addresses()
            if sas:
                data['StacktraceAddressSignature'] = sas
                metrics.meter('repair.succeeded_sas')
            else:
                metrics.meter('repair.failed_sas')

def submit(_pool, environ, system_token):
    counters_fam = pycassa.ColumnFamily(_pool, 'Counters',
                                        retry_counter_mutations=True)

    try:
        data = environ['wsgi.input'].read()
    except IOError as e:
        if e.message == 'request data read error':
            # The client disconnected while sending the report.
            metrics.meter('invalid.connection_dropped')
            return (False, 'Connection dropped.')
        else:
            raise
    try:
        data = bson.BSON(data).decode()
    except bson.errors.InvalidBSON:
        metrics.meter('invalid.invalid_bson')
        return (False, 'Invalid BSON.')

    # Keep a reference to the decoded report data. If we crash, we'll
    # potentially attach it to the OOPS report.
    environ['wsgi.input.decoded'] = data

    oops_id = str(uuid.uuid1())
    day_key = time.strftime('%Y%m%d', time.gmtime())

    if 'KernelCrash' in data or 'VmCore' in data:
        # We do not process these yet, but we keep track of how many reports
        # we're receiving to determine when it's worth solving.
        metrics.meter('unsupported.kernel_crash')
        return (False, 'Kernel crashes are not handled yet.')

    if len(data) == 0:
        metrics.meter('invalid.empty_report')
        return (False, 'Empty report.')

    # Write the SHA512 hash of the system UUID in with the report.
    if system_token:
        data['SystemIdentifier'] = system_token

    release = data.get('DistroRelease', '')
    eol_releases = {'Ubuntu 11.04': 'natty',
        'Ubuntu 11.10': 'oneiric',
        'Ubuntu 12.10': 'quantal',
        'Ubuntu 13.04': 'raring',
        'Ubuntu 13.10': 'saucy'}
    if release in eol_releases:
        metrics.meter('unsupported.eol_%s' % eol_releases[release])
        return (False, '%s is End of Life' % release)
    arch = data.get('Architecture', '')
    # We cannot retrace without an architecture to do it on
    if not arch:
        metrics.meter('missing.missing_arch')
    if arch == 'armel':
        metrics.meter('unsupported.armel')
        return (False, 'armel architecture is obsoleted')
    package = data.get('Package', '')
    pkg_arch = utils.get_package_architecture(data)
    src_package = data.get('SourcePackage', '')
    problem_type = data.get('ProblemType', '')
    exec_path = data.get('ExecutablePath', '')
    apport_version = data.get('ApportVersion', '')
    rootfs_build, device_image = utils.get_image_info(data)
    third_party = False
    if '[origin:' in package:
        third_party = True

    if not release:
        metrics.meter('missing.missing_release')
    if not package:
        metrics.meter('missing.missing_package')
    if not problem_type:
        metrics.meter('missing.missing_problem_type')
    if not exec_path:
        metrics.meter('missing.missing_executable_path')
    if exec_path.endswith('apportcheckresume'):
        # LP: #1316841 bad duplicate signatures
        if release == 'Ubuntu 14.04' and \
                apport_version == '2.14.1-0ubuntu3.1':
            metrics.meter('missing.missing_suspend_resume_data')
            return (False, 'Incomplete suspend resume data found in report.')
    else:
        metrics.meter('success.problem_type.%s' % problem_type)

    package, version = utils.split_package_and_version(package)
    src_package, src_version = utils.split_package_and_version(src_package)
    fields = utils.get_fields_for_bucket_counters(problem_type, release,
                                                  package, version, pkg_arch,
                                                  rootfs_build, device_image)

    if not third_party and problem_type == 'Crash':
        update_release_pkg_counter(counters_fam, release, src_package, day_key)
        if version == '':
            metrics.meter('missing.missing_package_version')
        else:
            update_release_pkg_version_counter(counters_fam, release, src_package, version, day_key)

    try_to_repair_sas(data)
    # ProcMaps is useful for creating a crash sig, not after that
    if 'Traceback' in data and 'ProcMaps' in data:
        data.pop('ProcMaps')
    # we only want this data after retracing with debug symbols
    if 'Stacktrace' in data:
        data.pop('Stacktrace')
    if 'ThreadStacktrace' in data:
        data.pop('ThreadStacktrace')
    if 'StacktraceTop' in data and 'Signal' in data:
        addr_sig = data.get('StacktraceAddressSignature', None)
        if not addr_sig and arch:
            metrics.meter('missing.missing_sas_%s' % arch)
    oopses.insert_dict(oops_config, oops_id, data, system_token, fields)
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    msg = '%s (%s) inserted into OOPS CF' % (now, oops_id)
    print >>sys.stderr, msg
    metrics.meter('success.oopses')

    success, output = bucket(_pool, oops_config, oops_id, data, day_key)
    return (success, output)

def bucket(_pool, oops_config, oops_id, data, day_key):
    '''Bucket oops_id.
       If the report was malformed, return (False, failure_msg)
       If a core file is to be requested, return (True, 'UUID CORE')
       If no further action is needed, return (True, 'UUID OOPSID')
    '''

    indexes_fam = pycassa.ColumnFamily(_pool, 'Indexes')
    stacktrace_cf = pycassa.ColumnFamily(_pool, 'Stacktrace')
    images_cf = pycassa.ColumnFamily(_pool, 'SystemImages')
    report = create_report_from_bson(data)

    # gather and insert image information in the SystemImages CF
    rootfs_build, device_image = utils.get_image_info(report)
    release = report.get('DistroRelease', '')
    if rootfs_build and release:
        # we include DistroRelease here but not in BucketVersionsCount, as it
        # is redundant in the counters
        rootfs_build = '%s:%s' % (release, rootfs_build)
        try:
            images_cf.get('rootfs_build', [rootfs_build])
        except NotFoundException:
            images_cf.insert('rootfs_build', {rootfs_build : ''})
    elif rootfs_build and not release:
        metrics.meter('missing.missing_release_has_rootfs_build')
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        msg = '%s (%s) rootfs_build without DistroRelease' % (now, oops_id)
        print >>sys.stderr, msg
    if device_image:
        try:
            images_cf.get('device_image', [device_image])
        except NotFoundException:
            images_cf.insert('device_image', {device_image : ''})

    # Recoverable Problem, Package Install Failure, Suspend Resume
    crash_signature = report.get('DuplicateSignature')
    if crash_signature:
        crash_signature = utils.format_crash_signature(crash_signature)
        utils.bucket(oops_config, oops_id, crash_signature, data)
        metrics.meter('success.duplicate_signature')
        return (True, '%s OOPSID' % oops_id)

    # Python
    crash_signature = report.crash_signature()
    if crash_signature and 'Traceback' in report:
        crash_signature = utils.format_crash_signature(crash_signature)
        utils.bucket(oops_config, oops_id, crash_signature, data)
        metrics.meter('success.python_bucketed')
        return (True, '%s OOPSID' % oops_id)

    # Binary
    if 'StacktraceTop' in data and 'Signal' in data:
        output = ''
        # we check for addr_sig before bucketing and inserting into oopses
        addr_sig = data.get('StacktraceAddressSignature', None)
        crash_sig = None
        try:
            crash_sig = indexes_fam.get(
                'crash_signature_for_stacktrace_address_signature', [addr_sig])
            crash_sig = crash_sig.values()[0]
        except (NotFoundException, KeyError, InvalidRequestException):
            pass
        # for some crashes apport isn't creating a Stacktrace in the
        # successfully retraced report, we need to retry these even though
        # there is a crash_sig
        stacktrace = False
        if addr_sig:
            try:
                traces = stacktrace_cf.get(addr_sig,
                                           columns=['Stacktrace',
                                                    'ThreadStacktrace'])
                if traces.get('Stacktrace', None) and \
                        traces.get('ThreadStacktrace', None):
                    stacktrace = True
            except NotFoundException:
                pass
        # only retry retracing failures that don't have third party packages
        # as those are likely to fail retracing
        retry = False
        if crash_sig:
            if crash_sig.startswith('failed:'):
                retry = True
        if 'third-party-packages' in data.get('Tags', ''):
            retry = False
        if crash_sig and not retry and stacktrace:
            # the crash is a duplicate so we don't need this data
            # Stacktrace, and ThreadStacktrace were already not accepted
            if 'ProcMaps' in report:
                unneeded_columns = ['Disassembly', 'ProcMaps', 'ProcStatus',
                                    'Registers', 'StacktraceTop']
                oops_cf = pycassa.ColumnFamily(_pool, 'OOPS')
                oops_cf.remove(oops_id, columns=unneeded_columns)
            # We have already retraced for this address signature, so this
            # crash can be immediately bucketed.
            utils.bucket(oops_config, oops_id, crash_sig, data)
            metrics.meter('success.ready_binary_bucketed')
        else:
            # Are we already waiting for this stacktrace address signature to
            # be retraced?
            waiting = True
            try:
                indexes_fam.get('retracing', [addr_sig])
            except (NotFoundException, InvalidRequestException):
                waiting = False

            release = data.get('DistroRelease', '')
            if not waiting and utils.retraceable_release(release):
                # retry SASes that failed to retrace as new dbgsym packages
                # may be available
                if crash_sig and retry:
                    metrics.meter('success.retry_failure')
                    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                    msg = '%s will retry: %s' % (now, oops_id)
                    print >>sys.stderr, msg
                # We do not have a core file in the queue, so ask for one. Do
                # not assume we're going to get one, so also add this ID the
                # the AwaitingRetrace CF queue as well.

                # We don't ask derivatives for core dumps. We could technically
                # check to make sure the Packages and Dependencies fields do not
                # have '[origin:' lines; however, apport-retrace looks for
                # configuration data in a directory named by the DistroRelease, so
                # these would always fail regardless.
                output = '%s CORE' % oops_id
                now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                msg = '%s (%s) asked for CORE' % (now, oops_id)
                print >>sys.stderr, msg
                metrics.meter('success.asked_for_core')

            awaiting_retrace_fam = pycassa.ColumnFamily(_pool, 'AwaitingRetrace')
            if addr_sig:
                awaiting_retrace_fam.insert(addr_sig, {oops_id : ''})
            metrics.meter('success.awaiting_binary_bucket')
        if not output:
            output = '%s OOPSID' % oops_id
        return (True, output)

    # Could not bucket
    could_not_bucket_fam = pycassa.ColumnFamily(_pool, 'CouldNotBucket')
    could_not_bucket_fam.insert(day_key, {uuid.UUID(oops_id): ''})
    return (True, '%s OOPSID' % oops_id)

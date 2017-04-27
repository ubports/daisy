#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright © 2011-2013 Canonical Ltd.
# Author: Evan Dandrea <evan.dandrea@canonical.com>
#         Brian Murray <brian.murray@canonical.com>
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

import bson
import hashlib
import logging
import os
import socket
import time
import uuid

from oopsrepository import config as oopsconfig
from oopsrepository import oopses
import pycassa
from pycassa.cassandra.ttypes import NotFoundException, InvalidRequestException
from pycassa.pool import MaximumRetryException

from daisy import config
import apport
from daisy import utils
from daisy.metrics import get_metrics

os.environ['OOPS_KEYSPACE'] = config.cassandra_keyspace
oops_config = oopsconfig.get_config()
oops_config['host'] = config.cassandra_hosts
oops_config['username'] = config.cassandra_username
oops_config['password'] = config.cassandra_password
oops_config['pool_size'] = config.cassandra_pool_size
oops_config['max_overflow'] = config.cassandra_max_overflow

metrics = get_metrics('daisy.%s' % socket.gethostname())
logger = logging.getLogger('gunicorn.error')


def update_release_pkg_counter(column_family, release, src_package, date):
    column_family.insert('%s:%s' % (release, src_package), {date: 1})

def update_release_pkg_version_counter(column_family, release, src_package, src_version, date):
    column_family.insert('%s:%s:%s' % (release, src_package, src_version), {date: 1})

def create_report_from_bson(data):
    report = apport.Report()
    for key in data:
        try:
            report[key.encode('UTF-8')] = data[key].encode('UTF-8')
        except AssertionError:
            # apport raises an AssertionError if a key is invalid, given that
            # the crash has already been written to the OOPS CF, skip the key
            # and continue bucketing
            metrics.meter('invalid.invalid_key')
            msg = 'Invalid key (%s) in report' % (key)
            logger.info(msg)
            continue
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
    proposed_counters_fam = pycassa.ColumnFamily(_pool, 'CountersForProposed',
                                                 retry_counter_mutations=True)
    systemoopshashes_cf = pycassa.ColumnFamily(_pool, 'SystemOOPSHashes')
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
        if not bson.is_valid(data):
            metrics.meter('invalid.invalid_bson')
            return (False, 'Invalid BSON.')
        data = bson.BSON(data).decode()
    except bson.errors.InvalidBSON, TypeError:
        metrics.meter('invalid.invalid_bson')
        return (False, 'Invalid BSON.')
    except MemoryError:
        metrics.meter('invalid.memory_error_bson')
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
    else:
        # we want to try and find out which releases are sending reports with
        # a missing SystemIdentifier
        try:
            whoopsie_version = environ['HTTP_X_WHOOPSIE_VERSION']
            metrics.meter('missing.missing_system_token_%s' % \
                whoopsie_version.replace('.', '_'))
        except KeyError:
            pass
        metrics.meter('missing.missing_system_token')

    release = data.get('DistroRelease', '')
    if release in utils.EOL_RELEASES:
        metrics.meter('unsupported.eol_%s' % utils.EOL_RELEASES[release])
        return (False, '%s is End of Life' % str(release))
    arch = data.get('Architecture', '')
    # We cannot retrace without an architecture to do it on
    if not arch:
        metrics.meter('missing.missing_arch')
    if arch == 'armel':
        metrics.meter('unsupported.armel')
        return (False, 'armel architecture is obsoleted.')
    # Check to see if the crash has already been reported
    date = data.get('Date', '')
    exec_path = data.get('ExecutablePath', '')
    proc_status = data.get('ProcStatus', '')
    if date and exec_path and proc_status and system_token:
        try:
            reported_crash_ids = systemoopshashes_cf.get(system_token)
            crash_id = '%s:%s:%s' % (date, exec_path, proc_status)
            if type(crash_id) == unicode:
                crash_id = crash_id.encode('utf-8')
            crash_id = hashlib.md5(crash_id).hexdigest()
            if crash_id in reported_crash_ids:
                return (False, 'Crash already reported.')
            try:
                whoopsie_version = environ['HTTP_X_WHOOPSIE_VERSION']
                metrics.meter('invalid.duplicate_report.whoopise_%s' % \
                    whoopsie_version.replace('.', '_'))
            except KeyError:
                pass
            metrics.meter('invalid.duplicate_report')
        except NotFoundException:
            pass
    package = data.get('Package', '')
    pkg_arch = utils.get_package_architecture(data)
    src_package = data.get('SourcePackage', '')
    problem_type = data.get('ProblemType', '')
    apport_version = data.get('ApportVersion', '')
    rootfs_build, channel, device_name, device_image = utils.get_image_info(data)
    third_party = False
    if not utils.retraceable_package(package):
        third_party = True
    automated_testing = False
    if system_token.startswith('deadbeef'):
        automated_testing = True

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
        failure = data.get('Failure', '')
        if failure == 'suspend/resume' and 'ProcMaps' in data:
            # this is not useful as it is from the resuming system
            data.pop('ProcMaps')
    else:
        metrics.meter('success.problem_type.%s' % problem_type)

    package, version = utils.split_package_and_version(package)
    # src_version is None and is never used, nor should it be.
    src_package, src_version = utils.split_package_and_version(src_package)
    fields = utils.get_fields_for_bucket_counters(problem_type, release,
                                                  package, version, pkg_arch,
                                                  rootfs_build, channel,
                                                  device_name, device_image)

    # generic counter for crashes about a source package which is used by the
    # phased-updater and only includes official Ubuntu packages and not those
    # from systems under auto testing.
    if not third_party and not automated_testing and problem_type == 'Crash':
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
        metrics.meter('missing.missing_sas')
    tags = data.get('Tags', '')

    package_from_proposed = False
    if 'package-from-proposed' in tags:
        package_from_proposed = True
        # generic counter for crashes about a source package which is used by
        # the phased-updater and only includes official Ubuntu packages and
        # not those from systems under auto testing.
        if not third_party and not automated_testing and problem_type == 'Crash':
            update_release_pkg_counter(proposed_counters_fam, release, src_package, day_key)
            if version != '':
                update_release_pkg_version_counter(proposed_counters_fam, release, src_package, version, day_key)

    # A device is manually blacklisted if it has repeatedly failed to have an
    # crash inserted into the OOPS table.
    if utils.blacklisted_device(system_token):
        # If the device stops appearing in the log file then the offending
        # crash file may have been removed and it could be unblacklisted.
        logger.info('Blacklisted device %s disallowed from sending a crash.' %
            system_token)
        return (False, 'Device blocked from sending crash reports.')

    try:
        oopses.insert_dict(oops_config, oops_id, data, system_token, fields,
                           proposed_pkg=package_from_proposed)
    except MaximumRetryException:
        msg = "%s: MaximumRetryException with %s keys." % \
              (system_token, len(data.keys()))
        logger.info(msg)
        logger.info("%s: The keys are %s" % (system_token, data.keys()))
        logger.info("%s: The crash has a ProblemType of: %s" % \
                    (system_token, problem_type))
        if 'Traceback' in data:
            logger.info("%s: The crash has a python traceback." % system_token)
        raise
    msg = '(%s) inserted into OOPS CF' % (oops_id)
    logger.info(msg)
    metrics.meter('success.oopses')
    if arch:
        metrics.meter('success.oopses.%s' % arch)

    success, output = bucket(_pool, oops_config, oops_id, data, day_key)
    return (success, output)

def bucket(_pool, oops_config, oops_id, data, day_key):
    '''Bucket oops_id.
       If the report was malformed, return (False, failure_msg)
       If a core file is to be requested, return (True, 'UUID CORE')
       If no further action is needed, return (True, 'UUID OOPSID')
    '''

    indexes_fam = pycassa.ColumnFamily(_pool, 'Indexes')
    oops_cf = pycassa.ColumnFamily(_pool, 'OOPS')
    stacktrace_cf = pycassa.ColumnFamily(_pool, 'Stacktrace')
    images_cf = pycassa.ColumnFamily(_pool, 'SystemImages')
    report = create_report_from_bson(data)

    # gather and insert image information in the SystemImages CF
    rootfs_build, channel, device_name, device_image = utils.get_image_info(data)
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
        msg = '(%s) rootfs_build without DistroRelease' % (oops_id)
        logger.info(msg)
    if channel:
        try:
            images_cf.get('channel', [channel])
        except NotFoundException:
            images_cf.insert('channel', {channel : ''})
    if device_name:
        try:
            images_cf.get('device_name', [device_name])
        except NotFoundException:
            images_cf.insert('device_name', {device_name : ''})
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
        oops_cf.insert(oops_id, {'DuplicateSignature': crash_signature})
        crash_signature = utils.format_crash_signature(crash_signature)
        utils.bucket(oops_config, oops_id, crash_signature, data)
        metrics.meter('success.python_bucketed')
        return (True, '%s OOPSID' % oops_id)

    # Crashing binary
    if 'StacktraceTop' in data and 'Signal' in data:
        output = ''
        # we check for addr_sig before bucketing and inserting into oopses
        addr_sig = data.get('StacktraceAddressSignature', None)
        # TODO: create a method to set retry = True for specific SASes
        # LP: #1505818
        crash_sig = ''
        try:
            crash_sig = indexes_fam.get(
                'crash_signature_for_stacktrace_address_signature', [addr_sig])
            crash_sig = crash_sig.values()[0]
        except (NotFoundException, KeyError, InvalidRequestException):
            pass
        failed_to_retrace = False
        if crash_sig.startswith('failed:'):
            failed_to_retrace = True
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
                metrics.meter('missing.missing_retraced_stacktrace')
                pass
        retry = False
        # If the retrace was successful but we don't have a stacktrace
        # something is wrong, so try retracing it again.
        if not failed_to_retrace and not stacktrace:
            retry = True
        # 2014-09-15 - given the quantity of crashes about i386 and amd64 only
        # retry crashes from armhf.
        arch = data.get('Architecture', '')
        if arch == 'armhf' and failed_to_retrace:
            retry = True
        if crash_sig and not retry:
            # The crash is a duplicate so we don't need this data.
            # Stacktrace, and ThreadStacktrace were already not accepted
            if 'ProcMaps' in report:
                unneeded_columns = ['Disassembly', 'ProcMaps', 'ProcStatus',
                                    'Registers', 'StacktraceTop']
                oops_cf.remove(oops_id, columns=unneeded_columns)
            # We have already retraced for this address signature, so this
            # crash can be immediately bucketed.
            utils.bucket(oops_config, oops_id, crash_sig, data)
            metrics.meter('success.ready_binary_bucketed')
            if arch:
                metrics.meter('success.ready_binary_bucketed.%s' % arch)
        else:
            # there is a bug with gdb when it tries to retrace crashes from
            # vim https://sourceware.org/bugzilla/show_bug.cgi?id=21324 so
            # don't ask for cores from it until its resolved.
            src_package = data.get('SourcePackage', '')
            if src_package == 'vim' and release == 'Ubuntu 16.10':
                return (True, '%s OOPSID' % oops_id)
            # apport requires the following fields to be able to retrace a crash
            # so do not ask for a CORE file if they don't exist
            if not release:
                return (True, '%s OOPSID' % oops_id)
            package = report.get('Package', '')
            if not package:
                return (True, '%s OOPSID' % oops_id)
            exec_path = report.get('ExecutablePath', '')
            if not exec_path:
                return (True, '%s OOPSID' % oops_id)
            # Are we already waiting for this stacktrace address signature to
            # be retraced?
            waiting = True
            try:
                indexes_fam.get('retracing', [addr_sig])
            except (NotFoundException, InvalidRequestException):
                waiting = False

            if not waiting and utils.retraceable_release(release):
                # there will not be a debug symbol version of the package so
                # don't ask for a CORE
                if not utils.retraceable_package(package):
                    metrics.meter('missing.retraceable_origin')
                    return (True, '%s OOPSID' % oops_id)
                # Don't ask for cores from things like google-chrome-stable
                # which will appear as "not installed" if installed from a
                # .deb
                if "(not installed)" in package:
                    metrics.meter('missing.package_version')
                    return (True, '%s OOPSID' % oops_id)
                # Do not ask for a core for crashes from click packages as we
                # don't have ddebs for them.
                click_package = report.get('ClickPackage', '')
                if click_package == "True":
                    metrics.meter('missing.click_package_ddeb')
                    return (True, '%s OOPSID' % oops_id)
                # retry SASes that failed to retrace as new dbgsym packages
                # may be available
                if crash_sig and retry:
                    metrics.meter('success.retry_failure')
                    msg = 'will retry retrace of: %s' % (oops_id)
                    logger.info(msg)
                elif crash_sig and not retry:
                    # Do not ask for a core for crashes we don't want to retry
                    metrics.meter('success.not_retry_failure')
                    return (True, '%s OOPSID' % oops_id)
                elif not addr_sig and not retry:
                    # Do not ask for a core for crashes without a SAS as they
                    # are likely corrupt cores.
                    metrics.meter('success.not_retry_no_sas')
                    return (True, '%s OOPSID' % oops_id)
                # We do not have a core file in the queue, so ask for one. Do
                # not assume we're going to get one, so also add this ID the
                # the AwaitingRetrace CF queue as well.

                # We don't ask derivatives for core dumps. We could technically
                # check to make sure the Packages and Dependencies fields do not
                # have '[origin:' lines; however, apport-retrace looks for
                # configuration data in a directory named by the DistroRelease, so
                # these would always fail regardless.
                output = '%s CORE' % oops_id
                msg = '(%s) asked for CORE' % (oops_id)
                logger.info(msg)
                metrics.meter('success.asked_for_core')
                if arch:
                    metrics.meter('success.asked_for_core.%s' % arch)
                if release:
                    metrics.meter('success.asked_for_core.%s' % release)

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

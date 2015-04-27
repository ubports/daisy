from amqplib.client_0_8.exceptions import AMQPConnectionException
from oopsrepository import oopses
import apt
import re
import socket
import uuid

# From oops-amqp
# These exception types always indicate an AMQP connection error/closure.
# However you should catch amqplib_error_types and post-filter with
# is_amqplib_connection_error.
amqplib_connection_errors = (socket.error, AMQPConnectionException)
# A tuple to reduce duplication in different code paths. Lists the types of
# exceptions legitimately raised by amqplib when the AMQP server goes down.
# Not all exceptions *will* be such errors - use is_amqplib_connection_error to
# do a second-stage filter after catching the exception.
amqplib_error_types = amqplib_connection_errors + (IOError,)

def get_fields_for_bucket_counters(problem_type, release, package, version,
        pkg_arch, rootfs_build, alias, device_name, device_image):
    fields = []
    if release:
        if package and version:
            fields.append('%s:%s:%s' % (release, package, version))
            fields.append('%s:%s' % (release, package))
            fields.append(release)
            fields.append('%s:%s' % (package, version))
            fields.append(package)
            if pkg_arch:
                fields.append('%s:%s:%s:%s' % (release, package, version, pkg_arch))
                fields.append('%s:%s:%s' % (release, package, pkg_arch))
                fields.append('%s:%s' % (release, pkg_arch))
                fields.append('%s:%s:%s' % (package, version, pkg_arch))
                fields.append('%s:%s' % (package, pkg_arch))
                fields.append('%s' % pkg_arch)
                if rootfs_build:
                    fields.append('%s:%s:%s:%s:%s' % (release, rootfs_build, package, version, pkg_arch))
                    fields.append('%s:%s:%s:%s' % (release, rootfs_build, package, pkg_arch))
                    fields.append('%s:%s:%s' % (release, rootfs_build, pkg_arch))
                    fields.append('%s:%s:%s:%s' % (rootfs_build, package, version, pkg_arch))
                    fields.append('%s:%s:%s' % (rootfs_build, package, pkg_arch))
                    fields.append('%s:%s' % (rootfs_build, pkg_arch))
                if alias:
                    fields.append('%s:%s:%s:%s:%s' % (release, alias, package, version, pkg_arch))
                    fields.append('%s:%s:%s:%s' % (release, alias, package, pkg_arch))
                    fields.append('%s:%s:%s' % (release, alias, pkg_arch))
                    fields.append('%s:%s:%s:%s' % (alias, package, version, pkg_arch))
                    fields.append('%s:%s:%s' % (alias, package, pkg_arch))
                    fields.append('%s:%s' % (alias, pkg_arch))
                    if device_name:
                        fields.append('%s:%s:%s:%s:%s:%s' % (release, alias, device_name, package, version, pkg_arch))
                        fields.append('%s:%s:%s:%s:%s' % (release, alias, device_name, package, pkg_arch))
                        fields.append('%s:%s:%s:%s' % (release, alias, device_name, pkg_arch))
                        fields.append('%s:%s:%s:%s:%s' % (alias, device_name, package, version, pkg_arch))
                        fields.append('%s:%s:%s:%s' % (alias, device_name, package, pkg_arch))
                        fields.append('%s:%s:%s' % (alias, device_name, pkg_arch))
                if device_image:
                    fields.append('%s:%s:%s:%s:%s' % (release, device_image, package, version, pkg_arch))
                    fields.append('%s:%s:%s:%s' % (release, device_image, package, pkg_arch))
                    fields.append('%s:%s:%s' % (release, device_image, pkg_arch))
                    fields.append('%s:%s:%s:%s' % (device_image, package, version, pkg_arch))
                    fields.append('%s:%s:%s' % (device_image, package, pkg_arch))
                    fields.append('%s:%s' % (device_image, pkg_arch))
            if rootfs_build:
                fields.append('%s:%s:%s:%s' % (release, rootfs_build, package, version))
                fields.append('%s:%s:%s' % (release, rootfs_build, package))
                fields.append('%s:%s' % (release, rootfs_build))
                fields.append('%s:%s:%s' % (rootfs_build, package, version))
                fields.append('%s:%s' % (rootfs_build, package))
                fields.append('%s' % (rootfs_build))
            if alias:
                fields.append('%s:%s:%s:%s' % (release, alias, package, version))
                fields.append('%s:%s:%s' % (release, alias, package))
                fields.append('%s:%s' % (release, alias))
                fields.append('%s:%s:%s' % (alias, package, version))
                fields.append('%s:%s' % (alias, package))
                fields.append('%s' % (alias))
                if device_name:
                    fields.append('%s:%s:%s:%s:%s' % (release, alias, device_name, package, version))
                    fields.append('%s:%s:%s:%s' % (release, alias, device_name, package))
                    fields.append('%s:%s:%s' % (release, alias, device_name))
                    fields.append('%s:%s:%s:%s' % (alias, device_name, package, version))
                    fields.append('%s:%s:%s' % (alias, device_name, package))
                    fields.append('%s:%s' % (alias, device_name))
            if device_image:
                fields.append('%s:%s:%s:%s' % (release, device_image, package, version))
                fields.append('%s:%s:%s' % (release, device_image, package))
                fields.append('%s:%s' % (release, device_image))
                fields.append('%s:%s:%s' % (device_image, package, version))
                fields.append('%s:%s' % (device_image, package))
                fields.append('%s' % (device_image))
        else:
            fields.append(release)
            # package w/o version is somewhat useful, version w/o package
            # isn't so only record this counter
            if package:
                fields.append('%s:%s' % (release, package))
                fields.append(package)
            if pkg_arch:
                fields.append('%s:%s' % (release, pkg_arch))
                fields.append('%s' % pkg_arch)
            if rootfs_build:
                fields.append('%s:%s:%s' % (release, rootfs_build, pkg_arch))
                fields.append('%s:%s' % (rootfs_build, pkg_arch))
                fields.append('%s' % (rootfs_build))
            if alias:
                fields.append('%s:%s:%s' % (release, alias, pkg_arch))
                fields.append('%s:%s' % (alias, pkg_arch))
                fields.append('%s' % (alias))
                if device_name:
                    fields.append('%s:%s:%s:%s' % (release, alias, device_name, pkg_arch))
                    fields.append('%s:%s:%s' % (alias, device_name, pkg_arch))
                    fields.append('%s:%s' % (alias, device_name))
            if device_image:
                fields.append('%s:%s:%s' % (release, device_image, pkg_arch))
                fields.append('%s:%s' % (device_image, pkg_arch))
                fields.append('%s' % (device_image))
    elif package and version:
        fields.append('%s:%s' % (package, version))
        fields.append('%s' % (package))
        if pkg_arch:
            fields.append('%s:%s:%s' % (package, version, pkg_arch))
            fields.append('%s:%s' % (package, pkg_arch))
            fields.append('%s' % pkg_arch)
            if rootfs_build:
                fields.append('%s:%s:%s:%s' % (rootfs_build, package, version, pkg_arch))
                fields.append('%s:%s:%s' % (rootfs_build, package, pkg_arch))
                fields.append('%s:%s' % (rootfs_build, pkg_arch))
            if alias:
                fields.append('%s:%s:%s:%s' % (alias, package, version, pkg_arch))
                fields.append('%s:%s:%s' % (alias, package, pkg_arch))
                fields.append('%s:%s' % (alias, pkg_arch))
                if device_name:
                    fields.append('%s:%s:%s:%s:%s' % (alias, device_name, package, version, pkg_arch))
                    fields.append('%s:%s:%s:%s' % (alias, device_name, package, pkg_arch))
                    fields.append('%s:%s:%s' % (alias, device_name, pkg_arch))
            if device_image:
                fields.append('%s:%s:%s:%s' % (device_image, package, version, pkg_arch))
                fields.append('%s:%s:%s' % (device_image, package, pkg_arch))
                fields.append('%s:%s' % (device_image, pkg_arch))
        if rootfs_build:
            fields.append('%s:%s:%s' % (rootfs_build, package, version))
            fields.append('%s:%s' % (rootfs_build, package))
            fields.append('%s' % (rootfs_build))
        if alias:
            fields.append('%s:%s:%s' % (alias, package, version))
            fields.append('%s:%s' % (alias, package))
            fields.append('%s' % (alias))
            if device_name:
                fields.append('%s:%s:%s:%s' % (alias, device_name, package, version))
                fields.append('%s:%s:%s' % (alias, device_name, package))
                fields.append('%s:%s' % (alias, device_name))
        if device_image:
            fields.append('%s:%s:%s' % (device_image, package, version))
            fields.append('%s:%s' % (device_image, package))
            fields.append('%s' % (device_image))


    if problem_type:
        fields.extend(['%s:%s' % (problem_type, field) for field in fields])
    return fields

def split_package_and_version(package):
    if not package:
        return ('', '')

    s = package.split()[:2]
    if len(s) == 2:
        package, version = s
    else:
        package, version = (package, '')
    if version == '(not':
        # The version is set to '(not installed)'
        version = ''
    return (package, version)

def get_package_architecture(report_dict):
    # return the system arch if the package is arch all
    pkg_arch = report_dict.get('PackageArchitecture', '')
    if pkg_arch == 'all':
        arch = report_dict.get('Architecture', '')
        pkg_arch = arch
    elif pkg_arch == 'unknown':
        pkg_arch = ''
    return pkg_arch

def get_image_info(report_dict):
    sysimage_info = report_dict.get('SystemImageInfo', '')
    if not sysimage_info:
        return (None, None, None, None)
    sii_dict = {}
    for line in sysimage_info.splitlines():
        sii_dict[line.split(':')[0].strip()] = ':'.join(line.split(':')[1:]).strip()
    rootfs_build = sii_dict.get('version ubuntu', '')
    channel = sii_dict.get('channel', '')
    alias = sii_dict.get('alias', '')
    version = sii_dict.get('version version', '')
    device_name = sii_dict.get('device name', '')
    if alias and version and device_name:
        device_image = '%s %s %s' % (alias, version, device_name)
    else:
        device_image = None
    return (rootfs_build, alias, device_name, device_image)

def format_crash_signature(crash_signature):
    # https://errors.ubuntu.com/oops-local/2013-03-07/50428.daisy.ubuntu.com3
    # Exception-Value: InvalidRequestException(why='Key length of 127727 is
    # longer than maximum of 65535')
    # We use 32768 rather than 65535 to provide padding when the bucket ID
    # forms part of a composite key, as it does in daybuckets.
    if not crash_signature:
        return ''

    # Translate back to unicode so we can correctly slice this.
    if type(crash_signature) == str:
        crash_signature = crash_signature.decode('utf-8')

    crash_signature = crash_signature[:32768]

    if type(crash_signature) == unicode:
        crash_signature = crash_signature.encode('utf-8')

    return crash_signature

def bucket(oops_config, oops_id, crash_signature, report_dict):
    release = report_dict.get('DistroRelease', '')
    package = report_dict.get('Package', '')
    src_package = report_dict.get('SourcePackage', '')
    problem_type = report_dict.get('ProblemType', '')
    dependencies = report_dict.get('Dependencies', '')
    system_uuid = report_dict.get('SystemIdentifier', '')

    if '[origin:' in package or '[origin:' in dependencies:
        # This package came from a third-party source. We do not want to show
        # its version as the Last Seen field on the most common problems table,
        # so skip updating the bucket metadata.
        third_party = True
    else:
        third_party = False

    version = None
    if package:
        package, version = split_package_and_version(package)
    pkg_arch = get_package_architecture(report_dict)
    rootfs_build, alias, device_name, device_image = get_image_info(report_dict)

    automated_testing = False
    if system_uuid.startswith('deadbeef'):
        automated_testing = True

    if automated_testing:
        fields = None
    else:
        fields = get_fields_for_bucket_counters(problem_type, release, package,
                                                version, pkg_arch, rootfs_build,
                                                alias, device_name, device_image)
    if version:
        oopses.update_bucket_systems(oops_config, crash_signature, system_uuid,
                                     version=version)
    # DayBucketsCount is only added to if fields is not None, so set fields to
    # None for crashes from systems running automated tests.
    oopses.bucket(oops_config, oops_id, crash_signature, fields)

    if hasattr(oopses, 'update_bucket_hashes'):
        oopses.update_bucket_hashes(oops_config, crash_signature)

    if (package and version) and release.startswith('Ubuntu '):
        oopses.update_bucket_metadata(oops_config, crash_signature, package,
                                      version, apt.apt_pkg.version_compare,
                                      release)
        if hasattr(oopses, 'update_source_version_buckets'):
            oopses.update_source_version_buckets(oops_config, src_package,
                                                 version, crash_signature)
    if version and release:
        oopses.update_bucket_versions(oops_config, crash_signature, version,
                                      release=release, oopsid=oops_id)

    if hasattr(oopses, 'update_errors_by_release'):
        if (system_uuid and release) and not third_party:
            oops_uuid = uuid.UUID(oops_id)
            oopses.update_errors_by_release(oops_config, oops_uuid, system_uuid, release)

def attach_error_report(report, context):
    # We only attach error report that was submitted by the client if we've hit
    # a MaximumRetryException from Cassandra.
    if 'type' in report and report['type'] == 'MaximumRetryException':
        env = context['wsgi_environ']
        if 'wsgi.input.decoded' in env:
            data = env['wsgi.input.decoded']
            if 'req_vars' not in report:
                report['req_vars'] = {}
            report['req_vars']['wsgi.input.decoded'] = data

def wrap_in_oops_wsgi(wsgi_handler):
    import oops_dictconfig
    from oops_wsgi import install_hooks, make_app
    from daisy import config
    cfg = oops_dictconfig.config_from_dict(config.oops_config)
    cfg.template['reporter'] = 'daisy'
    install_hooks(cfg)
    return make_app(wsgi_handler, cfg, oops_on_status=['500'])

def retraceable_release(release):
    release_re = re.compile('^Ubuntu( RTM)? \d\d.\d\d$')
    if release_re.match(release):
        return True
    else:
        return False

# From oops-amqp
def is_amqplib_ioerror(e):
    """Returns True if e is an amqplib internal exception."""
    # Raised by amqplib rather than socket.error on ssl issues and short reads.
    if not type(e) is IOError:
        return False
    if e.args == ('Socket error',) or e.args == ('Socket closed',):
        return True
    return False

# From oops-amqp
def is_amqplib_connection_error(e):
    """Return True if e was (probably) raised due to a connection issue."""
    return isinstance(e, amqplib_connection_errors) or is_amqplib_ioerror(e)

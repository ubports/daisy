from oopsrepository import oopses
import apt
import uuid

def get_fields_for_bucket_counters(problem_type, release, package, version):
    fields = []
    if release:
        if package and version:
            fields.append('%s:%s:%s' % (release, package, version))
            fields.append('%s:%s' % (release, package))
            fields.append(release)
            fields.append('%s:%s' % (package, version))
            fields.append(package)
        else:
            fields.append(release)
    elif package:
        fields.append('%s:%s' % (package, version))

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

    fields = get_fields_for_bucket_counters(problem_type, release, package, version)

    if version:
        oopses.update_bucket_systems(oops_config, crash_signature, version,
            system_uuid)

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
    if release.startswith('Ubuntu '):
        return True
    else:
        return False


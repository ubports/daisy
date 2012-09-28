from oops import Config
from oops_wsgi import install_hooks, make_app
from oops_datedir_repo import DateDirRepo, serializer_rfc822

from oopsrepository import oopses
import apt
import os

def get_fields_for_bucket_counters(release, package, version):
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

def bucket(oops_config, oops_id, crash_signature, report_dict):
    release = report_dict.get('DistroRelease', '')
    package = report_dict.get('Package', '')
    dependencies = report_dict.get('Dependencies', '')
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

    fields = get_fields_for_bucket_counters(release, package, version)
    oopses.bucket(oops_config, oops_id, crash_signature, fields)
    if (package and version) and not third_party:
        oopses.update_bucket_metadata(oops_config, crash_signature, package,
                                      version, apt.apt_pkg.version_compare,
                                      release)
    if version:
        oopses.update_bucket_versions(oops_config, crash_signature, version)

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

def wrap_in_oops_wsgi(wsgi_handler, path, hostname):
    config = Config()
    if not os.path.exists(path):
        os.mkdir(path)
    repo = DateDirRepo(path, hostname, serializer=serializer_rfc822)
    config.publishers.append(repo.publish)
    config.on_create.append(attach_error_report)
    install_hooks(config)
    return make_app(wsgi_handler, config, oops_on_status=['500'])

def retraceable_release(release):
    if release.startswith('Ubuntu '):
        # We only have ddeb sources for these releases.
        if release[7:] in ['10.04', '11.10', '12.04', '12.10']:
            return True
    return False
        

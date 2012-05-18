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

def bucket(oops_config, oops_id, crash_signature, report_dict):
    release = report_dict.get('DistroRelease', None)
    package = report_dict.get('Package', None)
    version = None
    if package:
        package, version = package.split()[:2]

    fields = get_fields_for_bucket_counters(release, package, version)
    oopses.bucket(oops_config, oops_id, crash_signature, fields)
    if package and version:
        oopses.update_bucket_metadata(oops_config, crash_signature, package,
                                      version, apt.apt_pkg.version_compare)

def wrap_in_oops_wsgi(wsgi_handler, path, hostname):
    config = Config()
    if not os.path.exists(path):
        os.mkdir(path)
    repo = DateDirRepo(path, hostname, serializer=serializer_rfc822)
    config.publishers.append(repo.publish)
    install_hooks(config)
    return make_app(wsgi_handler, config, oops_on_status=['500'])

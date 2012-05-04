from oopsrepository import oopses
import apt

def get_fields_for_bucket_counters(release, package, version):
    fields = []
    if release:
        if package and version:
            fields.append('%s:%s:%s' % (release, package, version))
            fields.append('%s:%s' % (release, package))
            fields.append(release)
            fields.append('%s:%s' % (package, version))
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

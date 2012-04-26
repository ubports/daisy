def get_fields_for_bucket_counters(report_dict):
    fields = []
    if 'DistroRelease' in report_dict:
        if 'Package' in report_dict:
            package, version = report_dict['Package'].split(' ')
            release = report_dict['DistroRelease']
            fields.append('%s:%s:%s' % (release, package, version))
            fields.append('%s:%s' % (release, package))
            fields.append(release)
            fields.append('%s:%s' % (package, version))
        else:
            fields.append(report_dict['DistroRelease'])
    elif 'Package' in report_dict:
        package, version = report_dict['Package'].split(' ')
        fields.append('%s:%s' % (package, version))
    return fields

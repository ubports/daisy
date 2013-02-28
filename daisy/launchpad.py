import apt
import httplib2
import json
import sys
import urllib
import urllib2

from lazr.restfulclient._browser import AtomicFileCache
from oauth import oauth

configuration = None
try:
    import local_config as configuration
except ImportError:
    pass
if not configuration:
    from daisy import configuration


if (not hasattr(configuration, 'lp_oauth_token') or
    not hasattr(configuration, 'lp_oauth_secret') or
    configuration.lp_oauth_token is None or
    configuration.lp_oauth_secret is None):
    raise ImportError('You must set lp_oauth_token and '
                      'lp_oauth_secret in local_config')

if configuration.lp_use_staging:
    _create_bug_url = 'https://api.qastaging.launchpad.net/devel/bugs'
    _ubuntu_target = 'https://api.qastaging.launchpad.net/devel/ubuntu'
    _oauth_realm = 'https://api.qastaging.launchpad.net'
else:
    _create_bug_url = 'https://api.launchpad.net/devel/bugs'
    _ubuntu_target = 'https://api.launchpad.net/devel/ubuntu'
    _oauth_realm = 'https://api.launchpad.net'

if configuration.lp_use_staging:
    _launchpad_base = 'https://api.qastaging.launchpad.net/devel'
else:
    _launchpad_base = 'https://api.launchpad.net/devel'

_get_published_binaries_url = (_launchpad_base + '/ubuntu/+archive/primary'
    '?ws.op=getPublishedBinaries&binary_name=%s'
    '&exact_match=true&ordered=true&status=Published')
_get_published_binary_version_url = (_launchpad_base + '/ubuntu/+archive/primary'
    '?ws.op=getPublishedBinaries&binary_name=%s&version=%s'
    '&exact_match=true&ordered=true&status=Published')
_get_bug_tasks_url = _launchpad_base + '/bugs/%s/bug_tasks'

_get_published_binaries_for_release_url = (_launchpad_base +
    '/ubuntu/+archive/primary/?ws.op=getPublishedBinaries&binary_name=%s'
    '&exact_match=true&distro_arch_series=%s')

_person_url = _launchpad_base + '/~'
_source_target = _launchpad_base + '/ubuntu/+source/'
_distro_arch_series = _launchpad_base + '/ubuntu/%s/i386'

# Bug and package lookup.

_file_cache = AtomicFileCache(configuration.http_cache_dir)
_http = httplib2.Http(_file_cache)


def json_request_entries(url):
    try:
        return json_request(url)['entries']
    except (KeyError, TypeError, ValueError):
        return ''

def json_request(url):
    try:
        response, content = _http.request(url)
    except httplib2.ServerNotFoundError:
        return ''

    return json.loads(content)


def get_all_codenames():
    url = _launchpad_base + '/ubuntu/series'
    return [entry['name'] for entry in json_request_entries(url)]


def get_codename_for_version(version):
    url = _launchpad_base + '/ubuntu/series'
    for entry in json_request_entries(url):
        if 'name' in entry and entry.get('version', None) == version:
            return entry['name']
    return None


def get_devel_series_codename():
    url = _launchpad_base + '/ubuntu/current_series'
    current_series = json_request(url)
    return current_series['name']


def get_version_for_codename(codename):
    url = _launchpad_base + '/ubuntu/series'
    for entry in json_request_entries(url):
        if entry['name'] == codename:
            return entry['version']
    return None


def get_versions_for_binary(binary_package, ubuntu_version):
    if not ubuntu_version:
        codenames = get_all_codenames()
    else:
        codenames = [get_codename_for_version(ubuntu_version)]
    if not codenames:
        return []
    # i386 and amd64 versions should be the same, hopefully.
    results = set()
    for codename in codenames:
        dist_arch = urllib.quote(_distro_arch_series % codename)
        url = _get_published_binaries_for_release_url % (binary_package, dist_arch)
        results |= set([x['binary_package_version'] for x in json_request_entries(url) if 'binary_package_version' in x])
    return sorted(results, cmp=apt.apt_pkg.version_compare)


def get_release_for_binary(binary_package, version):
    results = set()
    url = _get_published_binary_version_url % \
        (urllib.quote_plus(binary_package), urllib.quote_plus(version))
    results |= set([get_version_for_codename(x['display_name'].split(' ')[3]) for x in json_request_entries(url)])
    return results


def binaries_are_most_recent(specific_packages, release=None):
    '''For each (package, version) tuple supplied, determine if that is the
    most recent version of the binary package.

    This method lets us cache repeated lookups of the most recent version of
    the same binary package.'''

    _cache = {}
    result = []
    for package, version in specific_packages:
        if not package or not version:
            result.append(True)
            continue

        if package in _cache:
            latest_version = _cache[package]
        else:
            latest_version = _get_most_recent_binary_version(package, release)
            # We cache this even if _get_most_recent_binary_version returns
            # None, as packages like Skype will always return None and we
            # shouldn't keep asking.
            _cache[package] = latest_version

        if latest_version:
            r = apt.apt_pkg.version_compare(version, latest_version) != -1
            result.append(r)
        else:
            result.append(True)
    return result


def _get_most_recent_binary_version(package, release):
    url = _get_published_binaries_url % urllib.quote(package)
    if release:
        # TODO cache this by pushing it into the above function and instead
        # passing the distro_arch_series url.
        version = get_codename_for_version(release.split()[1])
        distro_arch_series = _distro_arch_series % version
        url += '&distro_arch_series=' + urllib.quote(distro_arch_series)
    try:
        return json_request_entries(url)[0]['binary_package_version']
    except (KeyError, IndexError):
        return ''


def binary_is_most_recent(package, version):
    # FIXME we need to factor in the release, otherwise this is often going to
    # look like the issue has disappeared when filtering the most common
    # problems view to a since-passed release.
    latest_version = _get_most_recent_binary_version(package)
    if not latest_version:
        return True
    # If the version we've been provided is older than the latest version,
    # return False; it's not the newest. We will then assume that because
    # we haven't seen it in the new version it may be fixed.
    return apt.apt_pkg.version_compare(version, latest_version) != -1


def bug_is_fixed(bug, release=None):
    url = _get_bug_tasks_url % urllib.quote(bug)
    if release:
        release = release.split()[1]
    codename = get_codename_for_version(release)
    if codename:
        codename_task = ' (ubuntu %s)' % codename
    else:
        codename_task = ''

    try:
        entries = json_request_entries(url)
        if len(entries) == 0:
            # We will presume that this is a private bug.
            return None

        for entry in entries:
            name = entry['bug_target_name']
            if release and codename and name.lower().endswith(codename_task):
                if not entry['is_complete']:
                    return False

        # Lets iterate again and see if we can find the Ubuntu task.
        for entry in entries:
            name = entry['bug_target_name']
            # Do not look at upstream bug tasks.
            if name.endswith(' (Ubuntu)'):
                # We also consider bugs that are Invalid as complete. I am not
                # entirely sure that is correct in this context.
                if not entry['is_complete']:
                    # As the bug itself may be in a library package bug task,
                    # it is not sufficient to return True at the first complete
                    # bug task.
                    return False
        return True
    except (ValueError, KeyError):
        return False


def is_source_package(package_name):
    dev_series = get_devel_series_codename()
    url = _launchpad_base + '/ubuntu/' + dev_series + \
        ('/?ws.op=getSourcePackage&name=%s' % package_name)
    request = json_request(url)
    if request:
        return True
    else:
        return False


def get_binaries_in_source_package(package_name, release=None):
    if not release:
        dev_series = get_devel_series_codename()
    else:
        dev_series = release
    package_name = urllib.quote_plus(package_name)
    ma_url = _launchpad_base + '/ubuntu/' + dev_series + '/main_archive'
    ma = json_request(ma_url)['self_link']
    dev_series_url = _launchpad_base + '/ubuntu/' + dev_series
    ps_url = ma + ('/?ws.op=getPublishedSources&exact_match=true&status=Published&source_name=%s&distro_series=%s' %
        (package_name, dev_series_url))
    # just use the first one, since they are unordered
    try:
        ps = json_request_entries(ps_url)[0]['self_link']
    except IndexError:
        return ''
    pb_url = ps + '/?ws.op=getPublishedBinaries'
    pbs = set(pb['binary_package_name'] for pb in json_request_entries(pb_url))
    return pbs

def urllib2_request_json(url, token, secret):
    headers = _generate_headers(token, secret)
    request = urllib2.Request(url, None, headers)
    response = urllib2.urlopen(request)
    content = response.read()
    return content

def get_subscribed_packages(user):
    '''return binary packages to which a user is subscribed'''
    src_pkgs = []
    bin_pkgs = []
    dev_series = get_devel_series_codename()
    url = _person_url + user + '?ws.op=getBugSubscriberPackages'
    json_data = urllib2_request_json(url, configuration.lp_oauth_token,
        configuration.lp_oauth_secret)
    try:
        tsl = json.loads(json_data)['total_size_link']
        total_size = int(urllib2_request_json(tsl, configuration.lp_oauth_token,
            configuration.lp_oauth_secret))
        while len(src_pkgs) < total_size:
            entries = json.loads(json_data)['entries']
            for entry in entries:
                src_pkgs.append(entry['name'])
            try:
                ncl = json.loads(json_data)['next_collection_link']
            except KeyError:
                break
            json_data = urllib2_request_json(ncl, configuration.lp_oauth_token,
                configuration.lp_oauth_secret)
    except KeyError:
        entries = json.loads(json_data)['entries']
        for entry in entries:
            src_pkgs.append(entry['name'])
    for src_pkg in src_pkgs:
        bin_pkgs.extend(list(get_binaries_in_source_package(src_pkg,
            dev_series)))
    return bin_pkgs


# Bug creation.


def _generate_operation(title, description, target=_ubuntu_target):
    operation = { 'ws.op' : 'createBug',
                  'description' : description,
                  'target' : target,
                  'title' : title }
    return urllib.urlencode(operation)


def _generate_headers(oauth_token, oauth_secret):
    a = (('OAuth realm="%s", '
          'oauth_consumer_key="testing", '
          'oauth_token="%s", '
          'oauth_signature_method="PLAINTEXT", '
          'oauth_signature="%%26%s", '
          'oauth_timestamp="%d", '
          'oauth_nonce="%s", '
          'oauth_version="1.0"') %
          (_oauth_realm, oauth_token, oauth_secret,
           int(oauth.time.time()), oauth.generate_nonce()))

    headers = { 'Authorization' : a,
                'Content-Type' : 'application/x-www-form-urlencoded' }
    return headers


def create_bug(signature, source=''):
    '''Returns a tuple of (bug number, url)'''

    title = '%s' % signature
    description = 'https://errors.ubuntu.com/bucket/?id=%s' % signature
    if source:
        target = _source_target + source
        operation = _generate_operation(title, description, target)
    else:
        operation = _generate_operation(title, description)
    headers = _generate_headers(configuration.lp_oauth_token,
                                configuration.lp_oauth_secret)

    # TODO Record the source packages and Ubuntu releases this crash has been
    # seen in, so we can add tasks for each relevant release.
    request = urllib2.Request(_create_bug_url, operation, headers)
    try:
        response = urllib2.urlopen(request)
    except urllib2.HTTPError as e:
        print >>sys.stderr, 'Could not create bug:', str(e)
        return (None, None)

    response.read()
    try:
        number = response.headers['Location'].rsplit('/', 1)[1]
        if configuration.lp_use_staging:
            return (number, 'https://qastaging.launchpad.net/bugs/' + number)
        else:
            return (number, 'https://bugs.launchpad.net/bugs/' + number)
    except KeyError:
        return (None, None)


def _generate_subscription(user):
    operation = {'ws.op': 'subscribe',
                 'level': 'Discussion',
                 'person': _person_url + user}
    return urllib.urlencode(operation)


def subscribe_user(bug, user):
    operation = _generate_subscription(user)
    headers = _generate_headers(configuration.lp_oauth_token,
                                configuration.lp_oauth_secret)
    url = '%s/%s' % (_create_bug_url, bug)
    request = urllib2.Request(url, operation, headers)
    try:
        urllib2.urlopen(request)
    except urllib2.HTTPError as e:
        msg = 'Could not subscribe %s to bug %s:' % (user, bug)
        print >>sys.stderr, msg, str(e), e.read()

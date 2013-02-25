from daisy import submit
from daisy import submit_core
from daisy import utils
from daisy import metrics
import re

config = None
try:
    import local_config as config
except ImportError:
    pass
if not config:
    import configuration as config

_pool = None
path_filter = re.compile('[^a-zA-Z0-9-_]')

def ok_response(start_response, data=''):
    if data:
        start_response('200 OK', [('Content-type', 'text/plain')])
    else:
        start_response('200 OK', [])
    return [data]

def bad_request_response(start_response, text=''):
    start_response('400 Bad Request', [])
    return [text]

def handle_core_dump(_pool, fileobj, components, content_type):
    l = len(components)
    operation = ''
    if l >= 4:
        # We also accept a system_hash parameter on the end of the URL, but do
        # not actually do anything with it.
        uuid, operation, arch = components[1:4]
    else:
        return (False, 'Invalid parameters')

    if not operation or operation != 'submit-core':
        # Unknown operation.
        return (False, 'Unknown operation')
    if content_type != 'application/octet-stream':
        # No data POSTed.
        # 'Incorrect Content-Type.'
        return (False, 'Incorrect Content-Type')

    uuid = path_filter.sub('', uuid)
    arch = path_filter.sub('', arch)

    return submit_core.submit(_pool, fileobj, uuid, arch)

def app(environ, start_response):
    global _pool
    if not _pool:
        _pool = metrics.failure_wrapped_connection_pool()

    path = environ.get('PATH_INFO', '')
    components = path.split('/')
    l = len(components)

    # There is only one path component with slashes either side.
    if ((l == 2 and not components[0]) or
        (l == 3 and not components[0] and not components[2])):
        # An error report submission.
        if len(components[1]) == 128:
            system_hash = components[1]
        else:
            system_hash = ''
        # We pass a reference to the wsgi environment so we can possibly attach
        # the decoded report to an OOPS report if an exception is raised.
        response = submit.submit(_pool, environ, system_hash)
    else:
        # A core dump submission.
        content_type = environ.get('CONTENT_TYPE', '')
        fileobj = environ['wsgi.input']
        response = handle_core_dump(_pool, fileobj, components, content_type)

    if response[0]:
        return ok_response(start_response, response[1])
    else:
        return bad_request_response(start_response, response[1])

oops_repo = config.oops_repository
application = utils.wrap_in_oops_wsgi(app, oops_repo, 'daisy.ubuntu.com')

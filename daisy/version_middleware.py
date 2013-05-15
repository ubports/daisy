from daisy.version import version_info
from oopsrepository import __version__

class VersionMiddleware(object):
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        def custom_start_response(status, headers, exc_info=None):
            if 'revno' in version_info:
                rev = version_info['revno']
                headers.append(['X-Daisy-Revision-Number', rev])
            ver = '.'.join(str(component) for component in __version__[0:3])
            headers.append(['X-Oops-Repository-Version', ver])
            return start_response(status, headers, exc_info)
        return self.app(environ, custom_start_response)

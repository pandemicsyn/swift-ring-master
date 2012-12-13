import eventlet
from os import stat
from eventlet import wsgi
from os.path import exists, join as pathjoin
from swift.common.utils import split_path, get_logger
from rms.utils import get_md5sum

class FileIterable(object):
    def __init__(self, filename):
        self.filename = filename

    def __iter__(self):
        return FileIterator(self.filename)


class FileIterator(object):

    chunk_size = 4096

    def __init__(self, filename):
        self.filename = filename
        self.fileobj = open(self.filename, 'rb')

    def __iter__(self):
        return self

    def next(self):
        chunk = self.fileobj.read(self.chunk_size)
        if not chunk:
            raise StopIteration
        return chunk

    __next__ = next

class FileLikeLogger(object):

    def __init__(self, logger):
        self.logger = logger

    def write(self, message):
        self.logger.info(message)

class RingMasterApp(object):
    """Ring Master wsgi app to serve up the ring the files"""

    def __init__(self, conf):
        self.ring_files = ['account.ring.gz', 'container.ring.gz',
                           'object.ring.gz']
        self.swiftdir = conf.get('swiftdir', '/etc/swift')
        self.wsgi_port = int(conf.get('serve_ring_port', '8090'))
        self.wsgi_address = conf.get('serve_ring_address', '')
        self.logger = get_logger(conf, 'ringmaster_wsgi')
        self.last_tstamp = {}
        self.current_md5 = {}
        for rfile in self.ring_files:
            target_file = pathjoin(self.swiftdir, rfile)
            if exists(target_file):
                self.last_tstamp[target_file] = stat(target_file).st_mtime
                self.current_md5[target_file] = get_md5sum(target_file)
            else:
                self.last_tstamp[target_file] = None
        self.request_logger = FileLikeLogger(self.logger)

    def _changed(self, filename):
        """Check if files been modified"""
        current = stat(filename).st_mtime
        if current == self.last_tstamp[filename]:
            return False
        else:
            return True

    def _validate_file(self, filename):
        """Validate md5 of file"""
        if self._changed(filename):
            self.logger.debug("updating md5")
            self.current_md5[filename] = get_md5sum(filename)

    def handle_ring(self, env, start_response):
        base, ringfile = split_path(env['PATH_INFO'], minsegs=1, maxsegs=2,
                                    rest_with_last=True)
        if ringfile not in self.ring_files:
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return ['Not Found\r\n']
        target = pathjoin(self.swiftdir, ringfile)
        try:
            self._validate_file(target)
        except (OSError, IOError):
            self.logger.exception('Oops')
            start_response('503 Service Unavailable',
                           [('Content-Type', 'text/plain')])
            return ['Service Unavailable\r\n']
        if 'HTTP_IF_NONE_MATCH' in env:
            if env['HTTP_IF_NONE_MATCH'] == self.current_md5[target]:
                headers = [('Content-Type', 'application/octet-stream')]
                start_response('304 Not Modified', headers)
                return ['Not Modified\r\n']
        if env['REQUEST_METHOD'] == 'GET':
            headers = [('Content-Type', 'application/octet-stream')]
            headers.append(('Etag', self.current_md5[target]))
            start_response('200 OK', headers)
            return FileIterable(target)
        elif env['REQUEST_METHOD'] == 'HEAD':
            headers = [('Content-Type', 'application/octet-stream')]
            headers.append(('Etag', self.current_md5[target]))
            start_response('200 OK', headers)
            return []
        else:
            start_response('501 Not Implemented', [('Content-Type',
                                                    'text/plain')])
            return ['Not Implemented\r\n']

    def handle_request(self, env, start_response):
        if env['PATH_INFO'].startswith('/ring/'):
            return self.handle_ring(env, start_response)
        else:
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return ['Not Found\r\n']

    def run(self):
        """fire up the app"""
        wsgi.server(eventlet.listen((self.wsgi_address, self.wsgi_port)),
                    self.handle_request, log=self.request_logger)
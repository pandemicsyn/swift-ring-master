import sys
import optparse
from os import stat
from os.path import exists, join as pathjoin
from eventlet import wsgi, listen
from swift.common.utils import split_path, readconf
from srm.utils import Daemon, get_md5sum, get_file_logger


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
        log_path = conf.get('log_path', '/var/log/ring-master/wsgi.log')
        self.logger = get_file_logger('ring-master-wsgi', log_path)
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
            self.last_tstamp[filename] = stat(filename).st_mtime
            self.current_md5[filename] = get_md5sum(filename)

    def handle_ring(self, env, start_response):
        """handle requests to /ring"""
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

    def start(self):
        """fire up the app"""
        wsgi.server(listen((self.wsgi_address, self.wsgi_port)),
                    self.handle_request, log=self.request_logger)


class RingMasterAppd(Daemon):

    def run(self, conf):
        rma = RingMasterApp(conf)
        rma.start()


def run_server():
    usage = '''
    %prog start|stop|restart [--conf=/path/to/some.conf] [--foreground|-f]
    '''
    args = optparse.OptionParser(usage)
    args.add_option('--foreground', '-f', action="store_true",
                    help="Run in foreground, in debug mode")
    args.add_option('--conf', default="/etc/swift/ring-master.conf",
                    help="path to config. default /etc/swift/ring-master.conf")
    args.add_option('--pid', default="/var/run/swift-ring-master-wsgi.pid",
                    help="default: /var/run/swift-ring-master-wsgi.pid")
    options, arguments = args.parse_args()

    if len(sys.argv) <= 1:
        args.print_help()

    if options.foreground:
        conf = readconf(options.conf)
        rma = RingMasterApp(conf['ringmaster_wsgi'])
        rma.start()
        sys.exit(0)

    if len(sys.argv) >= 2:
        conf = readconf(options.conf)
        user = conf['ringmaster_wsgi'].get('user', 'swift')
        daemon = RingMasterAppd(options.pid, user=user)
        if 'start' == sys.argv[1]:
            daemon.start(conf['ringmaster_wsgi'])
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart(conf['ringmaster_wsgi'])
        else:
            args.print_help()
            sys.exit(2)
        sys.exit(0)
    else:
        args.print_help()
        sys.exit(2)

if __name__ == '__main__':
    run_server()

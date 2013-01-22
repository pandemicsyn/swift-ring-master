import os
import sys
import optparse
from random import choice
from tempfile import mkstemp
from os.path import basename, dirname, join as pathjoin, exists
import eventlet
from eventlet.green import urllib2
from swift.common.utils import get_logger, readconf, TRUE_VALUES
from srm.utils import Daemon, get_md5sum, is_valid_ring


class RingMinion(object):

    def __init__(self, conf):
        self.current_md5 = {}
        self.swiftdir = conf.get('swiftdir', '/etc/swift')
        self.rings = {'account': conf.get('account_ring',
                                          pathjoin(self.swiftdir,
                                                   'account.ring.gz')),
                      'container': conf.get('container_ring',
                                            pathjoin(self.swiftdir,
                                                     'container.ring.gz')),
                      'object': conf.get('object_ring',
                                         pathjoin(self.swiftdir,
                                                  'object.ring.gz'))}
        self.start_delay = int(conf.get('start_delay_range', '120'))
        self.check_interval = int(conf.get('check_interval', '30'))
        self.ring_master = conf.get('ring_master', 'http://127.0.0.1:8090/')
        self.ring_master_timeout = int(conf.get('ring_master_timeout', '300'))
        self.debug = conf.get('debug', 'n') in TRUE_VALUES
        if self.debug:
            conf['log_level'] = 'DEBUG'
        self.logger = get_logger(conf, 'ringminiond', self.debug)
        for ring in self.rings:
            if exists(self.rings[ring]):
                self.current_md5[self.rings[ring]] = \
                    get_md5sum(self.rings[ring])
            else:
                continue

    def md5matches(self, target_file, expected_md5):
        if get_md5sum(target_file) == expected_md5:
            return True
        else:
            return False

    def ring_updated(self, ring_type):
        """update a ring

        :param ring_type: one of: account, container, or object.
        :param expected_md5: the expected md5sum of the retrieved ring file
        """
        url = "%sring/%s" % (self.ring_master, basename(self.rings[ring_type]))
        tmp = dirname(pathjoin(self.swiftdir, ring_type))
        headers = {'If-None-Match': self.current_md5[self.rings[ring_type]]}
        self.logger.info("Checking on %s ring to retrieve %s" % (ring_type,
                                                                 url))
        self.logger.debug("Using headers: %s" % headers)
        request = urllib2.Request(url, headers=headers)
        try:
            response = urllib2.urlopen(
                request, timeout=self.ring_master_timeout)
            if response.code == 200:
                expected_md5 = response.headers.get('etag')
                if not expected_md5:
                    self.logger.warning("No etag provided by ring-master")
                    return False
                fd, tmppath = mkstemp(dir=tmp, suffix='.tmp')
                with os.fdopen(fd, 'wb') as fdo:
                    while True:
                        chunk = response.read(4096)
                        if not chunk:
                            break
                        fdo.write(chunk)
                    fdo.flush()
                    os.fsync(fdo)
                    if self.md5matches(tmppath, expected_md5):
                        if not is_valid_ring(tmppath):
                            os.unlink(tmppath)
                            self.logger.error('error validating ring')
                            return False
                        os.chmod(tmppath, 0644)
                        os.rename(tmppath, self.rings[ring_type])
                        self.current_md5[self.rings[ring_type]] = expected_md5
                        return True
                    else:
                        self.logger.warning('md5 missmatch')
                        os.unlink(tmppath)
                        return False
            else:
                self.logger.warning('Got %s status with body:' % response.code)
                self.logger.warning(response.read())
                return False
        except urllib2.HTTPError, e:
            if e.code == 304:
                self.logger.debug('Ring-master reports ring unchanged.')
                return None
            else:
                self.logger.exception('Error communicating with ring-master')
                return False
        except urllib2.URLError:
            self.logger.exception('Error communicating with ring-master')
            return False

    def watch_loop(self):
        # insert a random delay on startup so we don't flood the server
        eventlet.sleep(choice(range(self.start_delay)))
        while True:
            try:
                for ring in self.rings:
                    changed = self.ring_updated(ring)
                    if changed:
                        self.logger.info("%s ring updated" % ring)
                    elif changed is False:
                        self.logger.info("%s ring check/change failed" % ring)
                    elif changed is None:
                        self.logger.info("%s ring remains unchanged" % ring)
                eventlet.sleep(self.check_interval)
            except Exception:
                self.logger.exception('Error watch loop')
                eventlet.sleep(self.check_interval)

    def once(self):
        for ring in self.rings:
            changed = self.ring_updated(ring)
            if changed:
                print "%s ring updated" % ring
            elif changed is False:
                print "%s ring change failed" % ring
            elif changed is None:
                print "%s ring remains unchanged" % ring


class RingMiniond(Daemon):

    def run(self, conf):
        """
        Startup Ring Minion Daemon
        """
        minion = RingMinion(conf)
        minion.watch_loop()


def run_server():
    usage = '''
    %prog start|stop|restart [--conf=/path/to/some.conf] [--foreground|-f]
    '''
    args = optparse.OptionParser(usage)
    args.add_option('--foreground', '-f', action="store_true",
                    help="Run in foreground, in debug mode")
    args.add_option('--once', '-o', action="store_true", help="Run once")
    args.add_option('--conf', default="/etc/swift/ring-minion.conf",
                    help="path to config. default /etc/swift/ring-minion.conf")
    options, arguments = args.parse_args()

    if len(sys.argv) <= 1:
        args.print_help()

    if options.foreground:
        conf = readconf(options.conf)
        minion = RingMinion(conf['minion'])
        if options.once:
            minion.once()
        else:
            minion.watch_loop()
        sys.exit(0)

    if len(sys.argv) >= 2:
        daemon = RingMiniond('/var/run/swift/swift-ring-minion-server.pid')
        if 'start' == sys.argv[1]:
            conf = readconf(options.conf)
            daemon.start(conf['minion'])
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            args.print_help()
            sys.exit(2)
        sys.exit(0)
    else:
        args.print_help()
        sys.exit(2)

if __name__ == '__main__':
    run_server()

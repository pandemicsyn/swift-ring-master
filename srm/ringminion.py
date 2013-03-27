"""
ring-minion
"""
import os
import sys
import urllib2
import optparse
from time import sleep
from random import choice
from tempfile import mkstemp
from os.path import basename, dirname, join as pathjoin, exists
from swift.common.utils import get_logger, readconf, TRUE_VALUES
from srm.utils import Daemon, get_md5sum, md5matches, is_valid_ring


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
        self.ring_master_timeout = int(conf.get('ring_master_timeout', '30'))
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

    def _write_ring(self, response, ring_type):
        """Write the ring out to a tmp file

        :param response: The urllib2 response to read from
        :param ring_type: The ring type we're working on
        :returns: path to tmp ring file"""
        tmp = dirname(pathjoin(self.swiftdir, ring_type))
        fd, tmppath = mkstemp(dir=tmp, suffix='.tmp')
        try:
            with os.fdopen(fd, 'wb') as fdo:
                while True:
                    chunk = response.read(4096)
                    if not chunk:
                        break
                    fdo.write(chunk)
                fdo.flush()
                os.fsync(fdo)
        except Exception:
            if tmppath:
                os.unlink(tmppath)
            raise
        return tmppath

    @staticmethod
    def _validate_ring(tmppath, expected_md5):
        """Make sure the ring is actually valid"""
        if not md5matches(tmppath, expected_md5):
            raise Exception('md5 missmatch')
        if not is_valid_ring(tmppath):
            raise Exception('Invalid ring')

    def _move_in_place(self, tmppath, ring_type, expected_md5):
        """Move the tmp ring into place"""
        os.chmod(tmppath, 0644)
        os.rename(tmppath, self.rings[ring_type])
        self.current_md5[self.rings[ring_type]] = expected_md5

    def fetch_ring(self, ring_type):
        """Fetch a new ring if theres one available

        :param ring_type: Ring to fetch object|container|account
        :returns: True on ring change, None for no change, False for error"""
        try:
            tmp_ring_path = None
            url = "%sring/%s" % (self.ring_master, basename(self.rings[ring_type]))
            headers = {'If-None-Match': self.current_md5[self.rings[ring_type]]}
            self.logger.debug("Checking on %s ring" % (ring_type))
            request = urllib2.Request(url, headers=headers)
            response = urllib2.urlopen(
                request, timeout=self.ring_master_timeout)
            if not response.code == 200:
                self.logger.warning('Received non 200 status code')
                return False
            tmp_ring_path = self._write_ring(response, ring_type)
            self._validate_ring(tmp_ring_path, response.headers.get('etag'))
            self._move_in_place(tmp_ring_path, ring_type,
                                response.headers.get('etag'))
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
        except Exception:
            if tmp_ring_path:
                try:
                    os.unlink(tmp_ring_path)
                except OSError:
                    pass
            self.logger.exception('Error retrieving or checking for new ring')
            return False
        return True

    def watch_loop(self):
        """Start monitoring ring files for changes"""
        # insert a random delay on startup so we don't flood the server
        sleep(choice(range(self.start_delay)))
        while True:
            try:
                for ring in self.rings:
                    changed = self.fetch_ring(ring)
                    if changed:
                        self.logger.info("%s updated" % ring)
                    elif changed is False:
                        self.logger.info("%s check/change failed!!" % ring)
                    elif changed is None:
                        self.logger.info("%s remains unchanged" % ring)
            except Exception:
                try:
                    self.logger.exception('Error in watch loop')
                except Exception:
                    print "Got exception and exception while trying to log"
            sleep(self.check_interval)

    def once(self):
        """Just check for changes once."""
        for ring in self.rings:
            changed = self.fetch_ring(ring)
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
        while True:
            try:
                minion.watch_loop()
            except Exception as err:
                #just in case
                print err


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
        conf = readconf(options.conf)
        user = conf['minion'].get('user', 'swift')
        out = '/tmp/oops.log'
        err = '/tmp/oops.log'
        daemon = RingMiniond('/var/run/swift/ring-minion-server.pid',
                             user=user, stdout=out, stderr=err)
        if 'start' == sys.argv[1]:
            daemon.start(conf['minion'])
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart(conf['minion'])
        else:
            args.print_help()
            sys.exit(2)
        sys.exit(0)
    else:
        args.print_help()
        sys.exit(2)

if __name__ == '__main__':
    run_server()

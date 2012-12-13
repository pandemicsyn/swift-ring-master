"""
Ring Master Daemon and Middleware for ring orchestration
"""

import sys
import eventlet
import optparse
import subprocess
import cPickle as pickle
from time import time
from datetime import datetime
from rms.ringmasterwsgi import RingMasterApp
from rms.utils import get_md5sum, make_backup, Daemon, is_valid_ring
from os import stat, unlink, rename, close, fdopen
from tempfile import mkstemp
from swift.common.ring import RingBuilder, Ring
from swift.common.utils import get_logger, readconf, TRUE_VALUES
from swift.common import exceptions

try:
    import simplejson as json
except ImportError:
    import json


class RingMasterServer(object):

    def __init__(self, rms_conf):
        conf = rms_conf['ringmasterd']
        self.swiftdir = conf.get('swiftdir', '/etc/swift')
        self.builder_files = \
            {'account': conf.get('account_builder',
                                 '/etc/swift/account.builder'),
             'container': conf.get('container_builder',
                                   '/etc/swift/container.builder'),
             'object': conf.get('object_builder',
                                '/etc/swift/object.builder')}
        self.ring_files = \
            {'account': conf.get('account_ring',
                                 '/etc/swift/account.ring.gz'),
             'container': conf.get('container_ring',
                                   '/etc/swift/container.ring.gz'),
             'object': conf.get('object_ring',
                                '/etc/swift/object.ring.gz')}
        self.debug = conf.get('debug_mode', 'n') in TRUE_VALUES
        self.oopmax = float(conf.get('oopmax', '5.0'))
        self.weight_shift = float(conf.get('weight_shift', '5.0'))
        self.backup_dir = conf.get('backup_dir', '/etc/swift/backups')
        self.recheck_interval = int(conf.get('interval', '120'))
        self.recheck_after_change_interval = int(conf.get('change_interval',
                                                          '120'))
        self.mph_enabled = conf.get('min_part_hours_check', 'n') in TRUE_VALUES
        self.sec_since_modified = int(conf.get('min_seconds_since_change',
                                               '10'))
        self.balance_threshold = int(conf.get('balance_threshold', '2'))
        self.dispersion_pct = {'container': float(conf.get('container_min_pct',
                                                           '99.50')),
                               'object': float(conf.get('object_min_pct',
                                                        '99.50'))}
        self.logger = get_logger(conf, 'ringmasterd', self.debug)
        self.serve_ring = rms_conf['ringmaster_wsgi'].get(
            'enabled', 'n') in TRUE_VALUES
        if self.serve_ring:
            self.wsgi_app = RingMasterApp(conf=rms_conf['ringmaster_wsgi'])
        else:
            self.wsgi_app = None

    def get_ring_hosts(self, zone_filter, swift_dir, ring_name):
        """
        Get a list of hosts in the ring

        :param zone_filter: Only list zones matching given filter
        :param swift_dir: Directory of swift config, usually /etc/swift
        :param ring_name: Name of the ring, such as 'object'
        :returns: a set of tuples containing the ip and port of hosts
        """
        ring_data = Ring(swift_dir, ring_name=ring_name)
        if zone_filter:
            ips = set((n['ip'], n['port']) for n in ring_data.devs if n
                      if n['zone'] == zone_filter)
        else:
            ips = set((n['ip'], n['port']) for n in ring_data.devs if n)
        return ips

    def rebalance_ring(self, builder):
        """Rebalance a ring

        :param builder: builder to rebalance
        :returns: True on successful rebalance, False if it fails.
        """
        devs_changed = builder.devs_changed
        try:
            last_balance = builder.get_balance()
            eventlet.sleep()
            parts, balance = builder.rebalance()
            eventlet.sleep()
        except exceptions.RingBuilderError:
            self.logger.error("-> Rebalance failed!")
            self.logger.exception('RingBuilderError')
            return False
        if not parts:
            self.logger.notice("-> No partitions reassigned!")
            self.logger.notice("-> (%d/%.02f)" % (parts, balance))
            return False
        if not devs_changed and abs(last_balance - balance) < 1:
            self.logger.notice("-> Rebalance failed to change more than 1%!")
            return False
        self.logger.notice('--> Reassigned %d (%.02f%%) partitions. Balance is %.02f.' %
                          (parts, 100.0 * parts / builder.parts, balance))
        return True

    def adjust_ring(self, builder):
        """Adjust device weights in a ring

        :param builder: builder to adjust
        """
        for dev in builder.devs:
            if 'target_weight' in dev:
                if dev['weight'] == dev['target_weight']:
                    continue
                elif dev['weight'] < dev['target_weight']:
                    if dev['weight'] + self.weight_shift \
                            < dev['target_weight']:
                        builder.set_dev_weight(
                            dev['id'], dev['weight'] + self.weight_shift)
                    else:
                        builder.set_dev_weight(dev['id'], dev['target_weight'])
                    if self.debug:
                        self.logger.notice(
                            "--> [%s/%s] ++ weight to %s" % (dev['ip'],
                                                             dev['device'],
                                                             dev['weight']))
                elif dev['weight'] > dev['target_weight']:
                    if dev['weight'] - self.weight_shift \
                            > dev['target_weight']:
                        builder.set_dev_weight(
                            dev['id'], dev['weight'] - self.weight_shift)
                    else:
                        builder.set_dev_weight(dev['id'], dev['target_weight'])
                    if self.debug:
                        self.logger.notice(
                            "--> [%s/%s] -- weight to %s" % (dev['ip'],
                                                             dev['device'],
                                                             dev['weight']))

    def ring_requires_change(self, builder):
        """Check if a ring requires changes

        :param builder: builder who's devices to check
        :returns: True if ring requires change
        """
        change = False
        for dev in builder.devs:
            if 'target_weight' in dev:
                if dev['weight'] != dev['target_weight']:
                    if self.debug:
                        self.logger.notice("--> [%s] weight %s | target %s"
                                           % (
                                           dev['ip'] + '/' +
                                           dev['device'], dev['weight'],
                                           dev['target_weight']))
                    change = True
        return change

    def dispersion_ok(self, swift_type):
        """Run a dispersion report and check whether its 'ok'

        :param swift_type: either 'container' or 'object'
        :returns: True if the dispersion report is 'ok'
        """
        if swift_type == 'account':
            return True
        if self.debug:
            self.logger.notice("--> Running %s dispersion report" % swift_type)
        dsp_cmd = ['swift-dispersion-report', '-j', '--%s-only' % swift_type]
        try:
            result = json.loads(subprocess.Popen(dsp_cmd,
                                stdout=subprocess.PIPE).communicate()[0])
        except Exception:
            self.logger.exception('Error running dispersion report')
            return False
        if not result[swift_type]:
            self.logger.notice("--> Dispersion report run returned nothing!")
            return False
        if self.debug:
            self.logger.notice("--> Dispersion info: %s" % result)
        if result[swift_type]['missing_2'] == 0 and \
                result[swift_type]['pct_found'] > \
                self.dispersion_pct[swift_type]:
            return True
        else:
            return False

    def min_part_hours_ok(self, builder):
        """Check if min part hours has elapsed

        :param builder: builder to check
        :returns: True if min part hours have elapsed
        """
        elapsed_hours = int(time() - builder._last_part_moves_epoch) / 3600
        if self.debug:
            self.logger.notice('--> partitions last moved %d hours ago [%s]'
                               % (elapsed_hours,
                                  datetime.utcfromtimestamp(builder._last_part_moves_epoch)))
        if elapsed_hours > builder.min_part_hours:
            return True
        else:
            return False

    def min_modify_time(self, btype):
        """Check if minimum modify time has passed

        :param builder: builder to check
        :returns: True if min modify time has elapsed
        """
        since_modified = time() - stat(self.builder_files[btype]).st_mtime
        if self.debug:
            self.logger.notice(
                '--> Ring last modified %d seconds ago.' % since_modified)
        if since_modified > self.sec_since_modified:
            return True
        else:
            return False

    def ring_balance_ok(self, builder):
        """Check if ring balance is ok

        :param builder: builder to check
        :returns: True ring balance is ok
        """
        if self.debug:
            self.logger.notice(
                '--> Current balance: %.02f' % builder.get_balance())
        if builder.get_balance() > self.balance_threshold:
            return False
        else:
            return True

    def write_builder(self, btype, builder):
        """Write out new ring files

        :param btype: The builder type
        :param builder: The builder to dump
        :returns: new ring file md5
        """
        builder_file = self.builder_files[btype]
        backup, backup_md5 = make_backup(builder_file, self.backup_dir)
        self.logger.notice(
            '--> Backed up %s to %s (%s)' % (builder_file, backup, backup_md5))
        fd, tmppath = mkstemp(dir=self.swiftdir, suffix='.tmp.builder')
        pickle.dump(builder.to_dict(), fdopen(fd, 'wb'), protocol=2)
        rename(tmppath, builder_file)
        return get_md5sum(builder_file)

    def write_ring(self, btype, builder):
        """Write out new ring files

        :param btype: The builder type
        :param builder: The builder to dump
        :returns: new ring file md5
        """
        ring_file = self.ring_files[btype]
        backup, backup_md5 = make_backup(ring_file, self.backup_dir)
        self.logger.notice('--> Backed up %s to %s (%s)' % (ring_file, backup,
                                                            backup_md5))
        fd, tmppath = mkstemp(dir=self.swiftdir, suffix='.tmp.ring.gz')
        builder.get_ring().save(tmppath)
        close(fd)
        if not is_valid_ring(tmppath):
            unlink(tmppath)
            raise Exception('Ring Validate Failed')
        rename(tmppath, ring_file)
        return get_md5sum(ring_file)

    def orchestration_pass(self):
        """Check the rings, make any needed adjustments, and deploy the ring"""
        ring_changed = False
        for btype in self.builder_files:
            if self.debug:
                self.logger.notice("=" * 79)
            self.logger.notice("Checking on %s ring..." % btype)
            self.logger.notice("=" * 79)
            builder = RingBuilder.load(self.builder_files[btype])
            if self.ring_requires_change(builder):
                self.logger.notice(
                    "[%s] -> ring requires weight change." % btype)

                if self.mph_enabled:
                    if not self.min_part_hours_ok(builder):
                        self.logger.notice(
                            "[%s] -> Ring min_part_hours: not ready!" % btype)
                        continue
                    else:
                        self.logger.notice(
                            "[%s] -> Ring min_part_hours: ok" % btype)

                if not self.min_modify_time(btype):
                    self.logger.notice(
                        "[%s] -> Ring last modify time: not ready!" % btype)
                    continue
                else:
                    self.logger.notice(
                        "[%s] -> Ring last modify time: ok" % btype)

                if not self.dispersion_ok(btype):
                    self.logger.notice(
                        "[%s] -> Dispersion report: not ready!" % btype)
                    continue
                else:
                    self.logger.notice("[%s] -> Dispersion report: ok" % btype)

                if self.ring_balance_ok(builder):
                    self.logger.notice(
                        "[%s] -> Current Ring balance: ok" % btype)
                    self.logger.notice("[%s] -> Adjusting ring..." % btype)
                    self.adjust_ring(builder)
                    self.logger.notice("[%s] -> Rebalancing ring..." % btype)
                    rebalanced = self.rebalance_ring(builder)
                    if not rebalanced:
                        self.logger.notice(
                            "[%s] -> Rebalance: not ready!" % btype)
                        continue
                    else:
                        self.logger.notice("[%s] -> Rebalance: ok" % btype)
                else:
                    self.logger.notice(
                        "[%s] -> Current Ring balance: not ready!" % btype)
                    self.logger.notice("[%s] -> Rebalancing ring with no modifications..." % btype)
                    rebalanced = self.rebalance_ring(builder)
                    if not rebalanced:
                        self.logger.notice(
                            "[%s] -> Rebalance: not ready!" % btype)
                        continue
                    else:
                        self.logger.notice("[%s] -> Rebalance: ok" % btype)
                self.logger.notice("[%s] -> Writing builder..." % btype)
                try:
                    builder_md5 = self.write_builder(btype, builder)
                    self.logger.notice("[%s] --> Wrote new builder with md5: %s" %
                                       (btype, builder_md5))
                    self.logger.notice("[%s] -> Writing ring..." % btype)
                    ring_md5 = self.write_ring(btype, builder)
                    self.logger.notice("[%s] --> Wrote new ring with md5: %s" %
                                       (btype, ring_md5))
                except Exception:
                    self.logger.exception('Error dumping builder or ring')
            else:
                self.logger.notice("[%s] -> No ring change required" % btype)
                continue
        if ring_changed:
            eventlet.sleep(self.recheck_after_change_interval)
        else:
            eventlet.sleep(self.recheck_interval)

    def run_wsgi(self):
        """Run the wsgi server to serve up the ring files"""
        while True:
            try:
                self.wsgi_app.run()
            except Exception:
                # sleep 60 seconds before we try again
                eventlet.sleep(60)
                self.logger.exception('Ring Master WSGI error')

    def start(self):
        """Start up the ring master"""
        self.logger.notice("Ring-Master starting up")
        if self.serve_ring:
            self.logger.notice("Spinning up wsgi server")
            eventlet.spawn_n(self.run_wsgi)
        self.logger.notice("-> Entering ring orchestration loop.")
        while True:
            try:
                self.orchestration_pass()
            except Exception:
                self.logger.exception('Orchestration Error')
                raise
            eventlet.sleep(1)


class RingMasterd(Daemon):

    def run(self, conf):
        """
        Startup Ring Management Daemon
        """
        rms = RingMasterServer(conf)
        rms.start()


def run_server():
    usage = '''
    %prog start|stop|restart [--conf=/path/to/some.conf] [--foreground|-f]
    '''
    args = optparse.OptionParser(usage)
    args.add_option('--foreground', '-f', action="store_true",
                    help="Run in foreground, in debug mode")
    args.add_option('--conf', default="/etc/swift/ring-master.conf",
                    help="path to config. default /etc/swift/ring-master.conf")
    options, arguments = args.parse_args()

    if len(sys.argv) <= 1:
        args.print_help()

    if options.foreground:
        conf = readconf(options.conf)
        tap = RingMasterServer(conf)
        tap.start()
        sys.exit(0)

    if len(sys.argv) >= 2:
        daemon = RingMasterd('/tmp/rmsd.pid')
        if 'start' == sys.argv[1]:
            conf = readconf(options.conf)
            daemon.start(conf)
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

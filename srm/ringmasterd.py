"""
Ring Master Daemon for ring orchestration
"""

import os
import sys
import optparse
import subprocess
import cPickle as pickle
from os.path import exists
from time import time, sleep
from tempfile import mkstemp
from datetime import datetime
from os import stat, unlink, rename, close, fdopen, chmod
from swift.common import exceptions
from swift.common.ring import RingBuilder
from swift.common.utils import get_logger, readconf, TRUE_VALUES, json, \
    lock_parent_directory
from srm.utils import get_md5sum, make_backup, Daemon, is_valid_ring, \
    EmailNotify


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
        self.pause_file = conf.get('pause_file_path', '/tmp/.srm-pause')
        self.default_weight_shift = float(conf.get('default_weight_shift',
                                                   '25.0'))
        self.backup_dir = conf.get('backup_dir', '/etc/swift/backups')
        self.recheck_interval = int(conf.get('interval', '120'))
        self.recheck_after_change_interval = int(conf.get('change_interval',
                                                          '3600'))
        self.mph_enabled = conf.get('min_part_hours_check', 'n') in TRUE_VALUES
        self.sec_since_modified = int(conf.get('min_seconds_since_change',
                                               '120'))
        self.balance_threshold = float(conf.get('balance_threshold', '2'))
        self.dispersion_cmd = {'dispersion_cmd':
                               '/usr/bin/swift-dispersion-report'}
        self.dispersion_pct = {'container': float(conf.get('container_min_pct',
                                                           '99.75')),
                               'object': float(conf.get('object_min_pct',
                                                        '99.75'))}
        self.lock_timeout = int(conf.get('lock_timeout', '90'))
        if self.debug:
            conf['log_level'] = 'DEBUG'
        self.logger = get_logger(conf, 'ringmasterd', self.debug)
        if not os.access(self.swiftdir, os.W_OK):
            self.logger.error('swift_dir is not writable. exiting!')
            sys.exit(1)
        if conf.get('email_notify', 'n') in TRUE_VALUES:
            self.email_notify = EmailNotify(conf, self.logger)
        else:
            self.email_notify = None

    def _emit_notify(self, source, message):
        "Send out any configured notifications"
        if self.email_notify:
            self.email_notify.send_message(source, message)

    def pause_if_asked(self):
        """Check if pause file exists and sleep until its removed if it does"""
        if exists(self.pause_file):
            self.logger.notice('--> Pause file found. Pausing orchestration!')
            while exists(self.pause_file):
                sleep(1)
            self.logger.notice('--> Pause removed. Resuming orchestration!')

    def rebalance_ring(self, builder):
        """Rebalance a ring

        :param builder: builder to rebalance
        :returns: True on successful rebalance, False if it fails.
        """
        self.pause_if_asked()
        devs_changed = builder.devs_changed
        try:
            last_balance = builder.get_balance()
            parts, balance = builder.rebalance()
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
        self.logger.notice('--> Reassigned %d (%.02f%%) partitions. Balance '
                           'is %.02f.' % (parts, 100.0 * parts / builder.parts,
                                          balance))
        return True

    def adjust_ring(self, builder):
        """Adjust device weights in a ring

        :param builder: builder to adjust
        """
        self.pause_if_asked()
        for dev in builder.devs:
            if not dev:
                continue
            if 'target_weight' in dev:
                if 'weight_shift' in dev:
                    weight_shift = dev['weight_shift']
                else:
                    weight_shift = self.default_weight_shift
                if dev['weight'] == dev['target_weight']:
                    continue
                elif dev['weight'] < dev['target_weight']:
                    if dev['weight'] + weight_shift \
                            < dev['target_weight']:
                        builder.set_dev_weight(
                            dev['id'], dev['weight'] + weight_shift)
                    else:
                        builder.set_dev_weight(dev['id'], dev['target_weight'])
                    self.logger.debug(
                        "--> [%s/%s] ++ weight to %s" % (dev['ip'],
                                                         dev['device'],
                                                         dev['weight']))
                elif dev['weight'] > dev['target_weight']:
                    if dev['weight'] - weight_shift \
                            > dev['target_weight']:
                        builder.set_dev_weight(
                            dev['id'], dev['weight'] - weight_shift)
                    else:
                        builder.set_dev_weight(dev['id'], dev['target_weight'])
                    self.logger.debug(
                        "--> [%s/%s] -- weight to %s" % (dev['ip'],
                                                         dev['device'],
                                                         dev['weight']))

    def ring_requires_change(self, builder):
        """Check if a ring requires changes

        :param builder: builder who's devices to check
        :returns: True if ring requires change
        """
        self.pause_if_asked()
        if builder.devs_changed:
            return True
        if not self.ring_balance_ok(builder):
            return True
        for dev in builder.devs:
            if not dev:
                continue
            if 'target_weight' in dev:
                if dev['weight'] != dev['target_weight']:
                    self.logger.debug("--> [%s] weight %s | target %s"
                                      % (
                                      dev['ip'] + '/' +
                                      dev['device'], dev['weight'],
                                      dev['target_weight']))
                    return True
        return False

    def dispersion_ok(self, swift_type):
        """Run a dispersion report and check whether its 'ok'

        :param swift_type: either 'container' or 'object'
        :returns: True if the dispersion report is 'ok'
        """
        self.pause_if_asked()
        if swift_type == 'account':
            return True
        self.logger.debug("--> Running %s dispersion report" % swift_type)
        dsp_cmd = [self.dispersion_cmd, '-j', '--%s-only' % swift_type]
        try:
            result = json.loads(subprocess.Popen(dsp_cmd,
                                stdout=subprocess.PIPE).communicate()[0])
        except Exception:
            self.logger.exception('Error running dispersion report')
            return False
        if not result[swift_type]:
            self.logger.notice("--> Dispersion report run returned nothing!")
            return False
        self.logger.debug("--> Dispersion info: %s" % result)
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
        self.pause_if_asked()
        elapsed_hours = int(time() - builder._last_part_moves_epoch) / 3600
        self.logger.debug('--> partitions last moved %d hours ago [%s]'
                          % (elapsed_hours, datetime.utcfromtimestamp(
                             builder._last_part_moves_epoch)))
        if elapsed_hours > builder.min_part_hours:
            return True
        else:
            return False

    def min_modify_time(self, btype):
        """Check if minimum modify time has passed

        :param btype: builder to check one of account|container|object
        :returns: True if min modify time has elapsed
        """
        self.pause_if_asked()
        since_modified = time() - stat(self.builder_files[btype]).st_mtime
        self.logger.debug(
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
        self.pause_if_asked()
        self.logger.debug(
            '--> Current balance: %.02f' % builder.get_balance())
        return builder.get_balance() <= self.balance_threshold

    def write_builder(self, btype, builder):
        """Write out new builder file

        :param btype: The builder type
        :param builder: The builder to dump
        :returns: new ring file md5
        """
        self.pause_if_asked()
        builder_file = self.builder_files[btype]
        try:
            fd, tmppath = mkstemp(dir=self.swiftdir, suffix='.tmp.builder')
            pickle.dump(builder.to_dict(), fdopen(fd, 'wb'), protocol=2)
            backup, backup_md5 = make_backup(builder_file, self.backup_dir)
            self.logger.notice('--> Backed up %s to %s (%s)' %
                              (builder_file, backup, backup_md5))
            chmod(tmppath, 0644)
            rename(tmppath, builder_file)
        except Exception as err:
            raise Exception('Error writing builder: %s' % err)
        finally:
            if fd:
                try:
                    close(fd)
                except OSError:
                    pass
            if tmppath:
                try:
                    unlink(tmppath)
                except OSError:
                    pass
        return get_md5sum(builder_file)

    def write_ring(self, btype, builder):
        """Write out new ring files

        :param btype: The builder type
        :param builder: The builder to dump
        :returns: new ring file md5
        """
        try:
            self.pause_if_asked()
            ring_file = self.ring_files[btype]
            fd, tmppath = mkstemp(dir=self.swiftdir, suffix='.tmp.ring.gz')
            builder.get_ring().save(tmppath)
            close(fd)
            if not is_valid_ring(tmppath):
                unlink(tmppath)
                raise Exception('Ring Validate Failed')
            backup, backup_md5 = make_backup(ring_file, self.backup_dir)
            self.logger.notice('--> Backed up %s to %s (%s)' %
                              (ring_file, backup, backup_md5))
            chmod(tmppath, 0644)
            rename(tmppath, ring_file)
        except Exception as err:
            raise Exception('Error writing builder: %s' % err)
        finally:
            if fd:
                try:
                    close(fd)
                except OSError:
                    pass
            if tmppath:
                try:
                    unlink(tmppath)
                except OSError:
                    pass
        return get_md5sum(ring_file)

    def orchestration_pass(self, btype):
        """Check the rings, make any needed adjustments, and deploy the ring

        :param btype: The builder type to work on.
        :return: True if the builder was modified , False if it was not
        """
        self.pause_if_asked()
        self.logger.debug("=" * 79)
        self.logger.notice("Checking on %s ring..." % btype)
        self.logger.debug("=" * 79)
        builder = RingBuilder.load(self.builder_files[btype])
        if self.ring_requires_change(builder):
            self.logger.notice("[%s] -> ring requires weight change." % btype)

            if self.mph_enabled:
                if not self.min_part_hours_ok(builder):
                    self.logger.notice(
                        "[%s] -> Ring min_part_hours: not ready!" % btype)
                    return False
                else:
                    self.logger.notice(
                        "[%s] -> Ring min_part_hours: ok" % btype)

            if not self.min_modify_time(btype):
                self.logger.notice(
                    "[%s] -> Ring last modify time: not ready!" % btype)
                return False
            else:
                self.logger.notice("[%s] -> Ring last modify time: ok" % btype)

            if not self.dispersion_ok(btype):
                self.logger.notice(
                    "[%s] -> Dispersion report: not ready!" % btype)
                return False
            else:
                self.logger.notice("[%s] -> Dispersion report: ok" % btype)

            if self.ring_balance_ok(builder):
                self.logger.notice("[%s] -> Current Ring balance: ok" % btype)
                self.logger.notice("[%s] -> Adjusting ring..." % btype)
                self.adjust_ring(builder)
                self.logger.notice("[%s] -> Rebalancing ring..." % btype)
                rebalanced = self.rebalance_ring(builder)
                if not rebalanced:
                    self.logger.notice("[%s] -> Rebalance: not ready!" % btype)
                    return True  # we should sleep a bit longer
                else:
                    self.logger.notice("[%s] -> Rebalance: ok" % btype)
            else:
                self.logger.notice(
                    "[%s] -> Current Ring balance: not ready!" % btype)
                self.logger.notice('[%s] -> Rebalancing ring with no '
                                   'modifications...' % btype)
                rebalanced = self.rebalance_ring(builder)
                if not rebalanced:
                    self.logger.notice(
                        "[%s] -> Rebalance: not ready!" % btype)
                    return True  # we should sleep a bit longer
                else:
                    self.logger.notice("[%s] -> Rebalance: ok" % btype)
            self.logger.notice("[%s] -> Writing builder..." % btype)
            try:
                builder_md5 = self.write_builder(btype, builder)
                self.logger.notice('[%s] --> Wrote new builder with md5: '
                                   '%s' % (btype, builder_md5))
                self.logger.notice("[%s] -> Writing ring..." % btype)
                ring_md5 = self.write_ring(btype, builder)
                self.logger.notice("[%s] --> Wrote new ring with md5: %s" %
                                   (btype, ring_md5))
                self._emit_notify('%s ring change' % btype,
                                  'Wrote new ring with md5: %s' % ring_md5)
                return True
            except Exception:
                self.logger.exception('Error dumping builder or ring')
        else:
            self.logger.notice("[%s] -> No ring change required" % btype)
            return False

    def start(self):
        """Start up the ring master"""
        self.logger.notice("Ring-Master starting up")
        self.logger.notice("-> Entering ring orchestration loop.")
        while True:
            try:
                self.pause_if_asked()
                for btype in self.builder_files:
                    with lock_parent_directory(self.builder_files[btype],
                                               self.lock_timeout):
                        ring_changed = self.orchestration_pass(btype)
                    if ring_changed:
                        sleep(self.recheck_after_change_interval)
                    else:
                        sleep(self.recheck_interval)
            except exceptions.LockTimeout:
                self.logger.exception('Orchestration LockTimeout Encountered')
            except Exception:
                self.logger.exception('Orchestration Error')
                sleep(60)
            sleep(1)


class RingMasterd(Daemon):

    def run(self, conf):
        """
        Startup Ring Management Daemon
        """
        rms = RingMasterServer(conf)
        rms.start()


def run_server():
    usage = '''
    %prog start|stop|restart|pause|unpause [--conf=/path/to/some.conf] [-f]
    '''
    args = optparse.OptionParser(usage)
    args.add_option('--foreground', '-f', action="store_true",
                    help="Run in foreground, in debug mode")
    args.add_option('--conf', default="/etc/swift/ring-master.conf",
                    help="path to config. default /etc/swift/ring-master.conf")
    args.add_option('--pid', default="/var/run/swift-ring-master.pid",
                    help="default: /var/run/swift-ring-master.pid")
    options, arguments = args.parse_args()

    if len(sys.argv) <= 1:
        args.print_help()

    if options.foreground:
        conf = readconf(options.conf)
        tap = RingMasterServer(conf)
        tap.start()
        sys.exit(0)

    if len(sys.argv) >= 2:
        conf = readconf(options.conf)
        user = conf['ringmasterd'].get('user', 'swift')
        pfile = conf['ringmasterd'].get('pause_file_path', '/tmp/.srm-pause')
        daemon = RingMasterd(options.pid, user=user)
        if 'start' == sys.argv[1]:
            daemon.start(conf)
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart(conf)
        elif 'pause' == sys.argv[1]:
            print "Writing pause file"
            with open(pfile, 'w') as f:
                f.write("")
        elif 'unpause':
            print "Removing pause file"
            unlink(pfile)
        else:
            args.print_help()
            sys.exit(2)
        sys.exit(0)
    else:
        args.print_help()
        sys.exit(2)

if __name__ == '__main__':
    run_server()

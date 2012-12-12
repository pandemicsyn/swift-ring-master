from hashlib import md5
from os import mkdir
from os.path import basename, join as pathjoin
from shutil import copy
from errno import EEXIST
import sys, os, time, atexit
from signal import SIGTERM


def get_md5sum(filename, chunk_size=4096):
    """Get the md5sum of a file

    :params filename: file to obtain the md5sum of
    :params chunk_size: chunk size
    :returns: hex digest of file
    """
    md5sum = md5()
    with open(filename, 'rb') as tfile:
        block = tfile.read(chunk_size)
        while block:
            md5sum.update(block)
            block = tfile.read(chunk_size)
    return md5sum.hexdigest()


def make_backup(filename, backup_dir):
    """ Create a backup of a file
    :params filename: The file to backup
    :params backup_dir: The directory where to backup the file
    :returns: List of backed up filename and md5sum of backed up file
    """
    try:
        mkdir(backup_dir)
    except OSError, err:
        if err.errno != EEXIST:
            raise
    backup = pathjoin(backup_dir, '%d.' % time() + basename(filename))
    copy(filename, backup)
    return [backup, get_md5sum(backup)]

# def get_logger(with_console=False):
#    logger = logging.getLogger('ringmasterd')
#    logger.setLevel(logging.DEBUG)
#    syslog = SysLogHandler(address='/dev/log')
#    syslog_formatter = logging.Formatter('%(name)s: %(message)s')
#    syslog.setFormatter(syslog_formatter)
#    logger.addHandler(syslog)
#    if with_console:
#        ch = logging.StreamHandler()
#        ch_formatter = logging.Formatter("%(message)s")
#        ch.setFormatter(ch_formatter)
#        ch.setLevel(logging.DEBUG)
#        logger.addHandler(ch)
#    return logger
#!/usr/bin/env python



#http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/
class Daemon:
    """
    A generic daemon class.

    Usage: subclass the Daemon class and override the run() method
    """

    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null',
                 stderr='/dev/null'):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile

    def daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError, err:
            sys.stderr.write("fork #1 failed: %d (%s)\n" %
                             (err.errno, err.strerror))
            sys.exit(1)

        # decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError, err:
            sys.stderr.write("fork #2 failed: %d (%s)\n" %
                             (err.errno, err.strerror))
            sys.exit(1)

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # write pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile, 'w+').write("%s\n" % pid)

    def delpid(self):
        os.remove(self.pidfile)

    def start(self, *args, **kw):
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        try:
            pidfile = file(self.pidfile, 'r')
            pid = int(pidfile.read().strip())
            pidfile.close()
        except IOError:
            pid = None

        if pid:
            message = "pidfile %s already exist. Daemon already running?\n"
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)

        # Start the daemon
        self.daemonize()
        self.run(*args, **kw)

    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pidfile = file(self.pidfile, 'r')
            pid = int(pidfile.read().strip())
            pidfile.close()
        except IOError:
            pid = None

        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return  # not an error in a restart

        try:
                while 1:
                        os.kill(pid, SIGTERM)
                        time.sleep(0.1)
        except OSError, err:
                err = str(err)
                if err.find("No such process") > 0:
                        if os.path.exists(self.pidfile):
                                os.remove(self.pidfile)
                else:
                        print str(err)
                        sys.exit(1)

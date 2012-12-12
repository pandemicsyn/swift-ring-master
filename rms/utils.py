from hashlib import md5
from os import mkdir
from os.path import basename, join as pathjoin
from shutil import copy
from errno import EEXIST
from time import time
# import logging
# from logging.handlers import SysLogHandler


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

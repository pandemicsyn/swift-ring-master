from mock import patch, Mock, MagicMock, sentinel, call
import unittest
from tempfile import mkdtemp
import cPickle as pickle
from swift.common.ring import RingBuilder
import time
import os
from shutil import rmtree
from rms.ringmasterd import RingMasterServer
from rms.utils import get_md5sum
import subprocess #topatch

class FakedBuilder(object):

    def __init__(self, device_count=5):
        self.device_count = device_count

    def gen_builder(self, balanced=False):
        builder = RingBuilder(18, 3, 1)
        for i in xrange(self.device_count):
            zone = i
            ipaddr = "1.1.1.1"
            port = 6010
            device_name = "sd%s" % i
            weight = 100.0
            meta = "meta for %s" % i
            next_dev_id = 0
            if builder.devs:
                next_dev_id = max(d['id'] for d in builder.devs if d) + 1
            builder.add_dev({'id': next_dev_id, 'zone': zone, 'ip': ipaddr,
                             'port': int(port), 'device': device_name,
                             'weight': weight, 'meta': meta})
            if balanced:
                builder.rebalance()
        return builder

    def write_builder(self, tfile, builder):
        pickle.dump(builder.to_dict(), open(tfile, 'wb'), protocol=2)


class test_ringmasterserver(unittest.TestCase):

    def setUp(self):
        self.testdir = mkdtemp()
        self.confdict = {'swiftdir': self.testdir,
                         'backup_dir': os.path.join(self.testdir, 'backup'),
                         'account_builder': os.path.join(self.testdir, 'account.builder'),
                         'container_builder': os.path.join(self.testdir, 'container.builder'),
                         'object_builder': os.path.join(self.testdir, 'object.builder'),
                         'account_ring': os.path.join(self.testdir, 'account.ring.gz'),
                         'container_ring': os.path.join(self.testdir, 'container.ring.gz'),
                         'object_ring': os.path.join(self.testdir, 'object.ring.gz')}

    def tearDown(self):
        try:
            rmtree(self.testdir)
        except Exception:
            pass

    def _setup_builder_rings(self, count=4, balanced=False):
        fb = FakedBuilder(device_count=count)
        builder = fb.gen_builder(balanced=balanced)
        for i in ['account.builder', 'container.builder', 'object.builder']:
            fb.write_builder(os.path.join(self.testdir, i), builder)
            ring_file = i[:-len('.builder')]
            ring_file += '.ring.gz'
            builder.get_ring().save(os.path.join(self.testdir, ring_file))

    def test_ring_requires_change(self):
        fb = FakedBuilder(device_count=4)
        builder = fb.gen_builder(balanced=False) 
        rmd = RingMasterServer(rms_conf={'ringmasterd': self.confdict})

        #test no change, with no target_weight
        self.assertFalse(rmd.ring_requires_change(builder))
        #test no change, with target_weight present but equal
        builder.devs[0]['target_weight'] = 100.0
        self.assertFalse(rmd.ring_requires_change(builder))
        #test with change
        builder.devs[0]['target_weight'] = 42.0
        self.assertTrue(rmd.ring_requires_change(builder))

    @patch('subprocess.Popen')
    def test_dispersion_ok(self, popen):
        popen.return_value = Mock()
        popen.return_value.returncode = 1
        popen.return_value.communicate = Mock()
        popen.return_value.communicate.return_value = ['some stderr']
        rmd = RingMasterServer(rms_conf={'ringmasterd': self.confdict})
        #test account
        self.assertTrue(rmd.dispersion_ok('account'))
        #test container
        #self.assertEquals(rmd.dispersion_ok('container'), '1')
        #test object
        

if __name__ == '__main__':
    unittest.main()

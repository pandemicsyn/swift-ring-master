import os
import unittest
import cPickle as pickle
from shutil import rmtree
from tempfile import mkdtemp
from mock import MagicMock, patch
from swift.common.ring import RingBuilder
from srm.ringminion import RingMinion
from srm.utils import get_md5sum
from StringIO import StringIO
import urllib2

class MockResponse(object):

    def __init__(self, resp_data="CRAP", code=200, msg='OK'):
        self.resp_data = resp_data
        self.code = code
        self.msg = msg
        self.headers = {'content-type': 'text/plain; charset=utf-8'}

    def read(self):
        return self.resp_data

    def getcode(self):
        return self.code


class FakeApp(object):
    def __call__(self, env, start_Response):
        return 'FakeApp'


class FakedBuilder(object):

    def __init__(self, device_count=4):
        self.device_count = device_count

    def gen_builder(self, balanced=False):
        builder = RingBuilder(8, 3, 1)
        for i in xrange(self.device_count):
            region = "1"
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
                             'weight': weight, 'meta': meta, 'region': region})
            if balanced:
                builder.rebalance()
        return builder

    def write_builder(self, tfile, builder):
        pickle.dump(builder.to_dict(), open(tfile, 'wb'), protocol=2)


class test_ringmasterminion(unittest.TestCase):

    def setUp(self):
        self.testdir = mkdtemp()
        self.patcher = patch('urllib2.urlopen')
        self.urlopen_mock = self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        try:
            rmtree(self.testdir)
        except Exception:
            pass

    def _setup_obj_ring(self, count=4, balanced=True):
        fb = FakedBuilder(device_count=count)
        builder = fb.gen_builder(balanced=balanced)
        fb.write_builder(os.path.join(self.testdir, 'object.builder'), builder)
        ring_file = 'object.ring.gz'
        builder.get_ring().save(os.path.join(self.testdir, ring_file))

    def test_validate_ring(self):
        self._setup_obj_ring()
        #ok ring and ok md5
        obj_ring = os.path.join(self.testdir, 'object.ring.gz')
        obj_ring_md5 = get_md5sum(obj_ring)
        minion = RingMinion(conf={'swiftdir': self.testdir})
        minion._validate_ring(obj_ring, obj_ring_md5)
        # m5 miss match
        try:
            minion._validate_ring(obj_ring, 'badmd5')
        except Exception as err:
            self.assertEqual(err.message, "md5 missmatch")
        else:
            self.fail('Should have thrown md5 missmatch exception')
        # bad ring file
        bfile = os.path.join(self.testdir, 'test.ring.gz')
        with open(bfile, 'w') as f:
            f.write('whatisthis.')
        test_md5 = get_md5sum(bfile)
        try:
            minion._validate_ring(bfile, test_md5)
        except Exception as err:
            self.assertEqual(err.message, "Invalid ring")
        else:
            self.fail('Should have thrown Invalid ring exception')

    @patch('srm.ringminion.RingMinion._write_ring')
    @patch('srm.ringminion.RingMinion._validate_ring')
    @patch('srm.ringminion.RingMinion._move_in_place')
    def test_fetch_ring(self, fmoveinplace, fvalidatering, fwritering):
        self._setup_obj_ring()
        #test non 200
        obj_ring = os.path.join(self.testdir, 'object.ring.gz')
        obj_ring_md5 = get_md5sum(obj_ring)
        minion = RingMinion(conf={'swiftdir': self.testdir})
        minion.logger = MagicMock()
        self.urlopen_mock.return_value = MockResponse(code=203)
        result = minion.fetch_ring('object')
        self.assertEquals(self.urlopen_mock.call_count, 1)
        self.assertFalse(result)
        urllib2.urlopen.assert_called_once
        minion.logger.warning.assert_called_once_with('Received non 200 status code')
        minion.logger.warning.reset_mock()
        self.urlopen_mock.reset_mock()
        #test 304
        self.urlopen_mock.side_effect = urllib2.HTTPError('http://a.com', 304, 'Nope', {}, None)
        minion.logger.debug.reset_mock()
        minion.logger.warning.reset_mock()
        minion.logger.exception.reset_mock()
        result = minion.fetch_ring('object')
        self.assertEquals(result, None)
        minion.logger.debug.assert_called_with('Ring-master reports ring unchanged.')
        minion.logger.debug.reset_mock()
        #test HTTPError non 304
        self.urlopen_mock.side_effect = urllib2.HTTPError('http://a.com', 401, 'GTFO', {}, None)
        minion.logger.debug.reset_mock()
        minion.logger.warning.reset_mock()
        minion.logger.exception.reset_mock()
        result = minion.fetch_ring('object')
        self.assertFalse(result)
        minion.logger.exception.assert_called_with('Error communicating with ring-master')
        minion.logger.exception.reset_mock()
        #test urllib2.URLError
        self.urlopen_mock.side_effect = urllib2.URLError('oops')
        minion.logger.debug.reset_mock()
        minion.logger.warning.reset_mock()
        minion.logger.exception.reset_mock()
        result = minion.fetch_ring('object')
        self.assertFalse(result)
        minion.logger.exception.assert_called_with('Error communicating with ring-master')
        minion.logger.exception.reset_mock()
        #test general exception or timeout
        self.urlopen_mock.side_effect = Exception('oopsie')
        minion.logger.debug.reset_mock()
        minion.logger.warning.reset_mock()
        minion.logger.exception.reset_mock()
        result = minion.fetch_ring('object')
        self.assertFalse(result)
        minion.logger.exception.assert_called_with('Error retrieving or checking for new ring')
        minion.logger.exception.reset_mock()

if __name__ == '__main__':
    unittest.main()

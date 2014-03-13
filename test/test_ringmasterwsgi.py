import os
import time
import unittest
import cPickle as pickle
from shutil import rmtree
from tempfile import mkdtemp
from mock import MagicMock
from swift.common.swob import Request
from swift.common.ring import RingBuilder
from swift.common.utils import lock_parent_directory
from swift.common.exceptions import LockTimeout
from srm.ringmasterwsgi import RingMasterApp
from srm.utils import get_md5sum


class FakeApp(object):
    def __call__(self, env, start_Response):
        return 'FakeApp'


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


class test_ringmasterwsgi(unittest.TestCase):

    def setUp(self):
        self.testdir = mkdtemp()
        self.test_log_path = os.path.join(self.testdir, 'wsgi-test.log')

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

    def test_ringmasterapp_methods(self):
        self._setup_builder_rings()
        rma = RingMasterApp({'swiftdir': self.testdir, 'log_path': self.test_log_path})
        for i in rma.current_md5:
            self.assertEquals(rma._changed(i), False)
        self._setup_builder_rings(count=5)
        for i in rma.current_md5:
            t = time.time() - 300
            os.utime(i, (t, t))
        for i in rma.current_md5:
            self.assertTrue(rma._changed(i))
            rma._validate_file(i)
            self.assertFalse(rma._changed(i))

    def test_ringmaster_validate_locked_dir(self):
        self._setup_builder_rings()
        rma = RingMasterApp({'swiftdir': self.testdir, 'log_path': self.test_log_path, 'locktimeout': "0.1"})
        for i in rma.current_md5:
            self.assertEquals(rma._changed(i), False)
        self._setup_builder_rings(count=5)
        for i in rma.current_md5:
            t = time.time() - 300
            os.utime(i, (t, t))
        with lock_parent_directory(self.testdir):
            for i in rma.current_md5:
                self.assertRaises(LockTimeout, rma._validate_file, i)

    def test_handle_request(self):
        self._setup_builder_rings()
        start_response = MagicMock()
        rma = RingMasterApp({'swiftdir': self.testdir, 'log_path': self.test_log_path})
        # test bad path
        req = Request.blank('/invalidrandomness',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = rma.handle_request(req.environ, start_response)
        start_response.assert_called_with(
            '404 Not Found', [('Content-Type', 'text/plain')])
        self.assertEquals(resp, ['Not Found\r\n'])
        # test legit path
        req = Request.blank('/ring/account.ring.gz',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = rma.handle_request(req.environ, start_response)
        account_md5 = get_md5sum(os.path.join(self.testdir, 'account.ring.gz'))
        start_response.assert_called_with('200 OK', [('Content-Type', 'application/octet-stream'), ('Etag', account_md5)])
        self.assertEquals(resp, [])

    def test_handle_ring(self):
        self._setup_builder_rings()
        start_response = MagicMock()
        rma = RingMasterApp({'swiftdir': self.testdir, 'log_path': self.test_log_path})

        # test bad path
        req = Request.blank('/ring/not_a_valid_ring.gz',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = rma.handle_ring(req.environ, start_response)
        start_response.assert_called_with(
            '404 Not Found', [('Content-Type', 'text/plain')])
        self.assertEquals(resp, ['Not Found\r\n'])

        # test bad method
        start_response.reset_mock()
        req = Request.blank('/ring/account.ring.gz',
                            environ={'REQUEST_METHOD': 'DELETE'})
        resp = rma.handle_ring(req.environ, start_response)
        start_response.assert_called_with(
            '501 Not Implemented', [('Content-Type', 'text/plain')])
        self.assertEquals(resp, ['Not Implemented\r\n'])

        # test HEAD
        start_response.reset_mock()
        req = Request.blank('/ring/account.ring.gz',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = rma.handle_ring(req.environ, start_response)
        account_md5 = get_md5sum(os.path.join(self.testdir, 'account.ring.gz'))
        start_response.assert_called_with('200 OK', [('Content-Type', 'application/octet-stream'), ('Etag', account_md5)])
        self.assertEquals(resp, [])

        # test GET w/ current If-None-Match
        start_response.reset_mock()
        account_md5 = get_md5sum(os.path.join(self.testdir, 'account.ring.gz'))
        req = Request.blank('/ring/account.ring.gz',
                            environ={'REQUEST_METHOD': 'GET',
                                     'HTTP_IF_NONE_MATCH': account_md5})
        resp = rma.handle_ring(req.environ, start_response)
        start_response.assert_called_with('304 Not Modified', [(
            'Content-Type', 'application/octet-stream')])
        self.assertEquals(resp, ['Not Modified\r\n'])

        # test GET w/ outdated If-None-Match
        start_response.reset_mock()
        account_md5 = get_md5sum(os.path.join(self.testdir, 'account.ring.gz'))
        req = Request.blank('/ring/account.ring.gz',
                            environ={'REQUEST_METHOD': 'GET',
                                     'HTTP_IF_NONE_MATCH': 'ihazaring'})
        resp = rma.handle_ring(req.environ, start_response)
        start_response.assert_called_with('200 OK', [('Content-Type', 'application/octet-stream'), ('Etag', account_md5)])
        testfile1 = os.path.join(self.testdir, 'gettest1.file')
        with open(testfile1, 'w') as f:
            for i in resp:
                f.write(i)
        self.assertTrue(account_md5, get_md5sum(testfile1))

        # test GET without If-None-Match
        start_response.reset_mock()
        account_md5 = get_md5sum(os.path.join(self.testdir, 'account.ring.gz'))
        req = Request.blank('/ring/account.ring.gz',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = rma.handle_ring(req.environ, start_response)
        start_response.assert_called_with('200 OK', [('Content-Type', 'application/octet-stream'), ('Etag', account_md5)])
        testfile2 = os.path.join(self.testdir, 'gettest2.file')
        with open(testfile2, 'w') as f:
            for i in resp:
                f.write(i)
        self.assertTrue(account_md5, get_md5sum(testfile2))

if __name__ == '__main__':
    unittest.main()

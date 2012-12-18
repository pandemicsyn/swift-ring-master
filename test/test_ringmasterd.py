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
import subprocess  # topatch
import json

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
                         'debug_mode': 'y',
                         'weight_shift': '5.0',
                         'interval': 1, 'change_interval': 2,
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

    def test_adjust_ring(self):
        fb = FakedBuilder(device_count=4)
        builder = fb.gen_builder(balanced=False)
        rmd = RingMasterServer(rms_conf={'ringmasterd': self.confdict})
        rmd.logger = MagicMock()
        #test no weight changes
        builder.devs[0]['target_weight'] = 100.0
        rmd.adjust_ring(builder)
        self.assertEquals(builder.devs[0]['weight'], 100.0)
        #test weight shift 1 inc
        builder.devs[0]['target_weight'] = 110.0
        rmd.adjust_ring(builder)
        self.assertEquals(builder.devs[0]['weight'], 105.0)
        #test weight shift partial increment
        builder.devs[0]['target_weight'] = 107.0
        rmd.adjust_ring(builder)
        self.assertEquals(builder.devs[0]['weight'], 107.0)
        #test weight shift down one increment
        builder.devs[1]['target_weight'] = 90.0
        rmd.adjust_ring(builder)
        self.assertEquals(builder.devs[1]['weight'], 95.0)
        #test weight shift down partial increment
        builder.devs[1]['target_weight'] = 92.0
        rmd.adjust_ring(builder)
        self.assertEquals(builder.devs[1]['weight'], 92.0)
        #test weight shift down an exact increment
        builder.devs[1]['target_weight'] = 87.0
        rmd.adjust_ring(builder)
        self.assertEquals(builder.devs[1]['weight'], 87.0)

    def test_ring_requires_change(self):
        fb = FakedBuilder(device_count=4)
        builder = fb.gen_builder(balanced=False)
        rmd = RingMasterServer(rms_conf={'ringmasterd': self.confdict})
        rmd.logger = MagicMock()
        # test no change, with no target_weight
        self.assertFalse(rmd.ring_requires_change(builder))
        # test no change, with target_weight present but equal
        builder.devs[0]['target_weight'] = 100.0
        self.assertFalse(rmd.ring_requires_change(builder))
        # test with change
        builder.devs[0]['target_weight'] = 42.0
        self.assertTrue(rmd.ring_requires_change(builder))

    @patch('subprocess.Popen')
    def test_dispersion_ok(self, popen):
        dsp_rpt = {"object": {"retries:": 0, "missing_2": 0,
                              "copies_found": 7863, "missing_1": 0,
                              "copies_expected": 7863, "pct_found": 100.0,
                              "overlapping": 0, "missing_all": 0},
                   "container": {"retries:": 0, "missing_2": 0,
                                 "copies_found": 12534, "missing_1": 0,
                                 "copies_expected": 12534,
                                 "pct_found": 100.0, "overlapping": 15,
                                 "missing_all": 0}}
        popen.return_value = Mock()
        popen.return_value.returncode = 1
        popen.return_value.communicate = Mock()
        popen.return_value.communicate.return_value = [json.dumps(dsp_rpt)]
        rmd = RingMasterServer(rms_conf={'ringmasterd': self.confdict})
        rmd.logger = MagicMock()
        # test account
        self.assertTrue(rmd.dispersion_ok('account'))
        self.assertEquals(rmd.logger.notice.call_count, 0)
        self.assertEquals(rmd.logger.exception.call_count, 0)
        rmd.logger.reset_mock()
        # test container and object ok
        self.assertTrue(rmd.dispersion_ok('container'))
        self.assertTrue(rmd.dispersion_ok('object'))
        self.assertEquals(rmd.logger.notice.call_count, 4)
        self.assertEquals(rmd.logger.exception.call_count, 0)
        rmd.logger.reset_mock()
        # test that container and obj fail on missing 2 replicas
        dsp_rpt_missing = dsp_rpt
        dsp_rpt_missing['container']['missing_2'] = 42
        dsp_rpt_missing['object']['missing_2'] = 42
        popen.return_value.communicate.return_value = [json.dumps(dsp_rpt_missing)]
        self.assertFalse(rmd.dispersion_ok('container'))
        self.assertFalse(rmd.dispersion_ok('object'))
        self.assertEquals(rmd.logger.notice.call_count, 4)
        self.assertEquals(rmd.logger.exception.call_count, 0)
        rmd.logger.reset_mock()
        # test that container and obj fail on missing large pct
        dsp_rpt_pct = dsp_rpt
        dsp_rpt_pct['container']['pct_found'] = 42.0
        dsp_rpt_pct['object']['pct_found'] = 42.0
        popen.return_value.communicate.return_value = [json.dumps(dsp_rpt_pct)]
        self.assertFalse(rmd.dispersion_ok('container'))
        self.assertFalse(rmd.dispersion_ok('object'))
        self.assertEquals(rmd.logger.notice.call_count, 4)
        self.assertEquals(rmd.logger.exception.call_count, 0)
        rmd.logger.reset_mock()
        # test catch exception
        popen.return_value.communicate.return_value = ''
        self.assertFalse(rmd.dispersion_ok('container'))
        self.assertFalse(rmd.dispersion_ok('object'))
        self.assertEquals(rmd.logger.notice.call_count, 2)
        self.assertEquals(rmd.logger.exception.call_count, 2)
        rmd.logger.reset_mock()
        # test no output
        popen.return_value.communicate.return_value = [json.dumps({'container': {},
                                                                   'object': {}})]
        self.assertFalse(rmd.dispersion_ok('container'))
        self.assertFalse(rmd.dispersion_ok('object'))
        self.assertEquals(rmd.logger.notice.call_count, 4)
        self.assertEquals(rmd.logger.exception.call_count, 0)
        rmd.logger.reset_mock()

    def test_min_modify_time(self):
        fb = FakedBuilder(device_count=4)
        builder = fb.gen_builder(balanced=False)
        pickle.dump(builder.to_dict(),
                    open(os.path.join(self.testdir, 'account.builder'), 'wb'),
                    protocol=2)
        rmd = RingMasterServer(rms_conf={'ringmasterd': self.confdict})
        rmd.logger = MagicMock()
        self.assertFalse(rmd.min_modify_time('account'))
        t = time.time() - 8600
        os.utime(os.path.join(self.testdir, 'account.builder'), (t, t))
        self.assertTrue(rmd.min_modify_time('account'))

    def test_ring_balance_ok(self):
        fb = FakedBuilder(device_count=4)
        builder = fb.gen_builder(balanced=False)
        rmd = RingMasterServer(rms_conf={'ringmasterd': self.confdict})
        rmd.logger = MagicMock()
        ok_balance = 0
        bad_balance = 42.0
        builder.get_balance = MagicMock(return_value=ok_balance)
        self.assertTrue(rmd.ring_balance_ok(builder))
        builder.get_balance.return_value = bad_balance
        self.assertFalse(rmd.ring_balance_ok(builder))

    @patch('eventlet.sleep')
    def test_orchestration_pass(self, evs):
        evs.return_value = Mock()
        self._setup_builder_rings(count=4, balanced=False)
        rmd = RingMasterServer(rms_conf={'ringmasterd': self.confdict})
        rmd.logger = MagicMock()
        rmd.ring_requires_change = MagicMock(return_value=True)
        rmd.min_part_hours_ok = MagicMock(return_value=True)
        rmd.min_modify_time = MagicMock(return_value=True)
        rmd.dispersion_ok = MagicMock(return_value=True)
        rmd.ring_balance_ok = MagicMock(return_value=True)
        rmd.rebalance_ring = MagicMock(return_value=True)
        rmd.write_builder = MagicMock(return_value=True)
        rmd.write_ring = MagicMock(return_value=True)

        def _reset_all():
            rmd.ring_requires_change.reset_mock()
            rmd.min_part_hours_ok.reset_mock()
            rmd.min_modify_time.reset_mock()
            rmd.dispersion_ok.reset_mock()
            rmd.ring_balance_ok.reset_mock()
            rmd.rebalance_ring.reset_mock()
            rmd.write_builder.reset_mock()
            rmd.write_ring.reset_mock()
            evs.reset_mock()

        #passes with no changes
        rmd.ring_requires_change.return_value = False
        rmd.orchestration_pass()
        self.assertFalse(rmd.min_part_hours_ok.called)
        self.assertFalse(rmd.min_modify_time.called)
        self.assertFalse(rmd.dispersion_ok.called)
        self.assertFalse(rmd.ring_balance_ok.called)
        self.assertFalse(rmd.rebalance_ring.called)
        self.assertFalse(rmd.write_builder.called)
        self.assertFalse(rmd.write_ring.called)
        self.assertEquals(evs.mock_calls, [call(rmd.recheck_interval)])
        _reset_all()

        #change required, with min_part_hours enabled and everything ready
        rmd.ring_requires_change.return_value = True
        rmd.mph_enabled = True
        rmd.orchestration_pass()
        self.assertTrue(rmd.min_part_hours_ok.called)
        self.assertTrue(rmd.min_modify_time.called)
        self.assertTrue(rmd.dispersion_ok.called)
        self.assertTrue(rmd.ring_balance_ok.called)
        self.assertTrue(rmd.rebalance_ring.called)
        self.assertTrue(rmd.write_builder.called)
        self.assertTrue(rmd.write_ring.called)
        self.assertEquals(evs.mock_calls,
                          [call(rmd.recheck_after_change_interval)])
        _reset_all()

        #change required, min_part_hours enabled and min_part_hours not ready
        rmd.ring_requires_change.return_value = True
        rmd.mph_enabled = True
        rmd.min_part_hours_ok.return_value = False
        rmd.orchestration_pass()
        self.assertTrue(rmd.min_part_hours_ok.called)
        self.assertFalse(rmd.min_modify_time.called)
        self.assertFalse(rmd.dispersion_ok.called)
        self.assertFalse(rmd.ring_balance_ok.called)
        self.assertFalse(rmd.rebalance_ring.called)
        self.assertFalse(rmd.write_builder.called)
        self.assertFalse(rmd.write_ring.called)
        self.assertEquals(evs.mock_calls, [call(rmd.recheck_interval)])
        rmd.min_part_hours_ok.return_value = True
        _reset_all()

        #change required, min_part_hours enabled and min_modify_time not ready
        rmd.ring_requires_change.return_value = True
        rmd.mph_enabled = True
        rmd.min_modify_time.return_value = False
        rmd.orchestration_pass()
        self.assertTrue(rmd.min_part_hours_ok.called)
        self.assertTrue(rmd.min_modify_time.called)
        self.assertFalse(rmd.dispersion_ok.called)
        self.assertFalse(rmd.ring_balance_ok.called)
        self.assertFalse(rmd.rebalance_ring.called)
        self.assertFalse(rmd.write_builder.called)
        self.assertFalse(rmd.write_ring.called)
        self.assertEquals(evs.mock_calls, [call(rmd.recheck_interval)])
        rmd.min_modify_time.return_value = True
        _reset_all()

        #change required, min_part_hours enabled and dispersion not ready
        rmd.ring_requires_change.return_value = True
        rmd.mph_enabled = True
        rmd.dispersion_ok.return_value = False
        rmd.orchestration_pass()
        self.assertTrue(rmd.min_part_hours_ok.called)
        self.assertTrue(rmd.min_modify_time.called)
        self.assertTrue(rmd.dispersion_ok.called)
        self.assertFalse(rmd.ring_balance_ok.called)
        self.assertFalse(rmd.rebalance_ring.called)
        self.assertFalse(rmd.write_builder.called)
        self.assertFalse(rmd.write_ring.called)
        self.assertEquals(evs.mock_calls, [call(rmd.recheck_interval)])
        rmd.dispersion_ok.return_value = True
        _reset_all()

        #change required, min_part_hours enabled and ring balance not ready
        rmd.ring_requires_change.return_value = True
        rmd.mph_enabled = True
        rmd.ring_balance_ok.return_value = False
        rmd.orchestration_pass()
        self.assertTrue(rmd.min_part_hours_ok.called)
        self.assertTrue(rmd.min_modify_time.called)
        self.assertTrue(rmd.dispersion_ok.called)
        self.assertTrue(rmd.ring_balance_ok.called)
        self.assertTrue(rmd.rebalance_ring.called)
        self.assertTrue(rmd.write_builder.called)
        self.assertTrue(rmd.write_ring.called)
        self.assertEquals(evs.mock_calls,
                          [call(rmd.recheck_after_change_interval)])
        rmd.ring_balance_ok.return_value = True
        _reset_all()

if __name__ == '__main__':
    unittest.main()
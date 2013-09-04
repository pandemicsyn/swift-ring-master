import os
import time
import subprocess  # to patch
import json
import unittest
import cPickle as pickle
from shutil import rmtree
from tempfile import mkdtemp
from swift.common import utils
from swift.common.ring import RingBuilder
from mock import patch, Mock, MagicMock, call
from srm.ringmasterd import RingMasterServer


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
        # add an empty dev
        builder.devs.append(None)
        if balanced:
            builder.rebalance()
        return builder

    def write_builder(self, tfile, builder):
        pickle.dump(builder.to_dict(), open(tfile, 'wb'), protocol=2)


class test_ringmasterserver(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = ''
        self.testdir = mkdtemp()
        self.confdict = {'swiftdir': self.testdir,
                         'debug_mode': 'y',
                         'default_weight_shift': '5.0',
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

    @patch('srm.ringmasterd.sleep')
    @patch('srm.ringmasterd.exists')
    def test_pause_if_asked(self, fexists, fsleep):
        fsleep.return_value = True
        fexists.side_effect = [True, True, False]
        rmd = RingMasterServer(rms_conf={'ringmasterd': self.confdict})
        rmd.logger = MagicMock()
        rmd.pause_if_asked()
        self.assertEquals(fsleep.call_count, 1)
        self.assertEquals(fexists.call_count, 3)
        self.assertEquals(rmd.logger.notice.call_count, 2)

    def test_adjust_ring(self):
        fb = FakedBuilder(device_count=4)
        builder = fb.gen_builder(balanced=False)
        rmd = RingMasterServer(rms_conf={'ringmasterd': self.confdict})
        rmd.logger = MagicMock()
        # test no weight changes
        builder.devs[0]['target_weight'] = 100.0
        rmd.adjust_ring(builder)
        self.assertEquals(builder.devs[0]['weight'], 100.0)
        # test weight shift 1 inc
        builder.devs[0]['target_weight'] = 110.0
        rmd.adjust_ring(builder)
        self.assertEquals(builder.devs[0]['weight'], 105.0)
        # test weight shift partial increment
        builder.devs[0]['target_weight'] = 107.0
        rmd.adjust_ring(builder)
        self.assertEquals(builder.devs[0]['weight'], 107.0)
        # test weight shift down one increment
        builder.devs[1]['target_weight'] = 90.0
        rmd.adjust_ring(builder)
        self.assertEquals(builder.devs[1]['weight'], 95.0)
        # test weight shift down partial increment
        builder.devs[1]['target_weight'] = 92.0
        rmd.adjust_ring(builder)
        self.assertEquals(builder.devs[1]['weight'], 92.0)
        # test weight shift down an exact increment
        builder.devs[1]['target_weight'] = 87.0
        rmd.adjust_ring(builder)
        self.assertEquals(builder.devs[1]['weight'], 87.0)
        # test weight shift with custom weight shift
        builder.devs[1]['target_weight'] = 70.0
        builder.devs[1]['weight_shift'] = 17
        rmd.adjust_ring(builder)
        self.assertEquals(builder.devs[1]['weight'], 70.0)

    def test_ring_requires_change(self):
        fb = FakedBuilder(device_count=4)
        builder = fb.gen_builder(balanced=False)
        builder.devs_changed = False
        rmd = RingMasterServer(rms_conf={'ringmasterd': self.confdict})
        rmd.ring_balance_ok = MagicMock(return_value=True)
        rmd.logger = MagicMock()
        # test no change, with no target_weight
        self.assertFalse(rmd.ring_requires_change(builder))
        # test no change, with target_weight present but equal
        builder.devs[0]['target_weight'] = 100.0
        self.assertFalse(rmd.ring_requires_change(builder))
        # test with change
        builder.devs[0]['target_weight'] = 42.0
        self.assertTrue(rmd.ring_requires_change(builder))
        # test brand new ring (i.e. newly added devs)
        builder = fb.gen_builder(balanced=False)
        self.assertTrue(rmd.ring_requires_change(builder))
        # test brand new ring bad balance
        builder.devs_changed = False
        rmd.ring_balance_ok = MagicMock(return_value=False)
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
        self.assertEquals(rmd.logger.debug.call_count, 4)
        self.assertEquals(rmd.logger.exception.call_count, 0)
        rmd.logger.reset_mock()
        # test that container and obj are ok on missing a small pct
        dsp_rpt_spct = dsp_rpt
        dsp_rpt_spct['container']['pct_found'] = 99.9995
        dsp_rpt_spct['object']['pct_found'] = 99.9995
        popen.return_value.communicate.return_value = [json.dumps(dsp_rpt_spct)]
        print json.dumps(dsp_rpt_spct)
        self.assertTrue(rmd.dispersion_ok('container'))
        self.assertTrue(rmd.dispersion_ok('object'))
        self.assertEquals(rmd.logger.debug.call_count, 4)
        self.assertEquals(rmd.logger.exception.call_count, 0)
        rmd.logger.reset_mock()
        # test that container and obj fail on missing 2 replicas
        dsp_rpt_missing = dsp_rpt
        dsp_rpt_missing['container']['missing_2'] = 42
        dsp_rpt_missing['object']['missing_2'] = 42
        popen.return_value.communicate.return_value = [json.dumps(
            dsp_rpt_missing)]
        self.assertFalse(rmd.dispersion_ok('container'))
        self.assertFalse(rmd.dispersion_ok('object'))
        self.assertEquals(rmd.logger.debug.call_count, 4)
        self.assertEquals(rmd.logger.exception.call_count, 0)
        rmd.logger.reset_mock()
        # test that container and obj fail on missing large pct
        dsp_rpt_pct = dsp_rpt
        dsp_rpt_pct['container']['missing_2'] = 0
        dsp_rpt_pct['object']['missing_2'] = 0
        dsp_rpt_pct['container']['pct_found'] = 42.0
        dsp_rpt_pct['object']['pct_found'] = 42.0
        popen.return_value.communicate.return_value = [json.dumps(dsp_rpt_pct)]
        self.assertFalse(rmd.dispersion_ok('container'))
        self.assertFalse(rmd.dispersion_ok('object'))
        self.assertEquals(rmd.logger.debug.call_count, 4)
        self.assertEquals(rmd.logger.exception.call_count, 0)
        rmd.logger.reset_mock()
        # test catch exception
        popen.return_value.communicate.return_value = ''
        self.assertFalse(rmd.dispersion_ok('container'))
        self.assertFalse(rmd.dispersion_ok('object'))
        self.assertEquals(rmd.logger.debug.call_count, 2)
        self.assertEquals(rmd.logger.exception.call_count, 2)
        rmd.logger.reset_mock()
        # test no output
        popen.return_value.communicate.return_value = [json.dumps(
            {'container': {},
             'object': {}})]
        self.assertFalse(rmd.dispersion_ok('container'))
        self.assertFalse(rmd.dispersion_ok('object'))
        self.assertEquals(rmd.logger.debug.call_count, 2)
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

    def test_min_part_hours_ok(self):
        fb = FakedBuilder(device_count=4)
        builder = fb.gen_builder(balanced=False)
        builder._last_part_moves_epoch = int(time.time())
        rmd = RingMasterServer(rms_conf={'ringmasterd': self.confdict})
        rmd.logger = MagicMock()
        self.assertFalse(rmd.min_part_hours_ok(builder))
        builder._last_part_moves_epoch = int(time.time()) - 424242
        self.assertTrue(rmd.min_part_hours_ok(builder))

    # fixme - wtf am i doing here
    @patch('srm.ringmasterd.chmod')
    @patch('srm.ringmasterd.rename')
    @patch('srm.ringmasterd.mkstemp')
    @patch('srm.ringmasterd.make_backup')
    @patch('srm.ringmasterd.get_md5sum')
    @patch('srm.ringmasterd.pickle.dump')
    @patch('srm.ringmasterd.close')
    def test_write_builder(self, fdclose, pd, gmd5, mbackup, ftmp, frename,
                           fake_chmod):
        fake_chmod.return_value = True
        frename.return_value = True
        ftmp.return_value = [1, '/fake/path/a.file']
        mbackup.return_value = ['testit', 'somemd5']
        gmd5.return_value = True
        pd.return_value = Mock()
        fdclose.return_value = Mock()
        fb = FakedBuilder(device_count=4)
        builder = fb.gen_builder(balanced=False)
        builder.devs_changed = False
        rmd = RingMasterServer(rms_conf={'ringmasterd': self.confdict})
        rmd.swiftdir = os.path.realpath('.')
        rmd.logger = MagicMock()
        self.assertTrue(rmd.write_builder('object', builder))
        self.assertEquals(ftmp.mock_calls, [call(
            suffix='.tmp.builder', dir=os.path.realpath('.'))])
        fake_builder_path = os.path.join(self.testdir, 'object.builder')
        fake_builder_bdir = os.path.join(self.testdir, 'backup')
        self.assertEquals(
            mbackup.mock_calls, [call(fake_builder_path, fake_builder_bdir)])
        self.assertEquals(fdclose.mock_calls, [call(1)])
        ftmp.return_value = [2, '/fake/path/a.file']
        mbackup.side_effect = Exception('OMGMONKEY!')
        self.assertRaises(Exception, rmd.write_builder, ['something', 'else'])

    @patch('srm.ringmasterd.sleep')
    def test_orchestration_pass(self, srs):
        srs.return_value = Mock()
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
            srs.reset_mock()

        # passes with no changes
        rmd.ring_requires_change.return_value = False
        ring_changed = rmd.orchestration_pass('object')
        self.assertFalse(rmd.min_part_hours_ok.called)
        self.assertFalse(rmd.min_modify_time.called)
        self.assertFalse(rmd.dispersion_ok.called)
        self.assertFalse(rmd.ring_balance_ok.called)
        self.assertFalse(rmd.rebalance_ring.called)
        self.assertFalse(rmd.write_builder.called)
        self.assertFalse(rmd.write_ring.called)
        self.assertFalse(ring_changed)
        _reset_all()

        # change required, with min_part_hours enabled and everything ready
        rmd.ring_requires_change.return_value = True
        rmd.mph_enabled = True
        ring_changed = rmd.orchestration_pass('object')
        self.assertTrue(rmd.min_part_hours_ok.called)
        self.assertTrue(rmd.min_modify_time.called)
        self.assertTrue(rmd.dispersion_ok.called)
        self.assertTrue(rmd.ring_balance_ok.called)
        self.assertTrue(rmd.rebalance_ring.called)
        self.assertTrue(rmd.write_builder.called)
        self.assertTrue(rmd.write_ring.called)
        self.assertTrue(ring_changed)
        _reset_all()

        # change required, min_part_hours enabled and min_part_hours not ready
        rmd.ring_requires_change.return_value = True
        rmd.mph_enabled = True
        rmd.min_part_hours_ok.return_value = False
        ring_changed = rmd.orchestration_pass('object')
        self.assertTrue(rmd.min_part_hours_ok.called)
        self.assertFalse(rmd.min_modify_time.called)
        self.assertFalse(rmd.dispersion_ok.called)
        self.assertFalse(rmd.ring_balance_ok.called)
        self.assertFalse(rmd.rebalance_ring.called)
        self.assertFalse(rmd.write_builder.called)
        self.assertFalse(rmd.write_ring.called)
        self.assertFalse(ring_changed)
        rmd.min_part_hours_ok.return_value = True
        _reset_all()

        # change required, min_part_hours enabled and min_modify_time not ready
        rmd.ring_requires_change.return_value = True
        rmd.mph_enabled = True
        rmd.min_modify_time.return_value = False
        ring_changed = rmd.orchestration_pass('object')
        self.assertTrue(rmd.min_part_hours_ok.called)
        self.assertTrue(rmd.min_modify_time.called)
        self.assertFalse(rmd.dispersion_ok.called)
        self.assertFalse(rmd.ring_balance_ok.called)
        self.assertFalse(rmd.rebalance_ring.called)
        self.assertFalse(rmd.write_builder.called)
        self.assertFalse(rmd.write_ring.called)
        self.assertFalse(ring_changed)
        rmd.min_modify_time.return_value = True
        _reset_all()

        # change required, min_part_hours enabled and dispersion not ready
        rmd.ring_requires_change.return_value = True
        rmd.mph_enabled = True
        rmd.dispersion_ok.return_value = False
        ring_changed = rmd.orchestration_pass('object')
        self.assertTrue(rmd.min_part_hours_ok.called)
        self.assertTrue(rmd.min_modify_time.called)
        self.assertTrue(rmd.dispersion_ok.called)
        self.assertFalse(rmd.ring_balance_ok.called)
        self.assertFalse(rmd.rebalance_ring.called)
        self.assertFalse(rmd.write_builder.called)
        self.assertFalse(rmd.write_ring.called)
        self.assertFalse(ring_changed)
        rmd.dispersion_ok.return_value = True
        _reset_all()

        # change required, min_part_hours enabled and ring balance not ready
        rmd.ring_requires_change.return_value = True
        rmd.mph_enabled = True
        rmd.ring_balance_ok.return_value = False
        ring_changed = rmd.orchestration_pass('object')
        self.assertTrue(rmd.min_part_hours_ok.called)
        self.assertTrue(rmd.min_modify_time.called)
        self.assertTrue(rmd.dispersion_ok.called)
        self.assertTrue(rmd.ring_balance_ok.called)
        self.assertTrue(rmd.rebalance_ring.called)
        self.assertTrue(rmd.write_builder.called)
        self.assertTrue(rmd.write_ring.called)
        self.assertTrue(ring_changed)
        rmd.ring_balance_ok.return_value = True
        _reset_all()

if __name__ == '__main__':
    unittest.main()

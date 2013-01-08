from swift.common.ring import RingBuilder
from eventlet import sleep
import cPickle as pickle
from srm import ringmasterd

def modring():
    builder = RingBuilder.load('/etc/swift/object.builder')
    builder.devs[0]['target_weight'] = 80.0
    builder._last_part_moves_epoch = builder._last_part_moves_epoch - 86400
    pickle.dump(builder.to_dict(), open('/etc/swift/object.builder', 'wb'), protocol=2)

def fudge_epoch():
    builder = RingBuilder.load('/etc/swift/object.builder')
    builder._last_part_moves_epoch = builder._last_part_moves_epoch - 86400
    pickle.dump(builder.to_dict(), open('/etc/swift/object.builder', 'wb'), protocol=2)

def runit():
    modring()
    sleep(10)
    reload(ringmasterd)
    ringmasterd.RingMasterServer(conf=None).start()

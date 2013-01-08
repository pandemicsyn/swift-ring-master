Requirements
============

## Software
- Swift (at least swift.common.ring and swift.common.utils, and access to swift-dispersion-report)
- eventlet (for wsgi app/server)
- mock (for unittests)

## On an SAIO
- A working swift-dispersion-report (have ya ran -populate ?)
- A ring with reasonable weights (that you can increment/decrement and rebalance)
- A copy of /etc/swift in /etc/swift-test or the like that you can point the swift-ring-minion-server to
- A handy note about ring-master.conf settings for testing would be handy here :/

## Testing ring orchestration

1. On a working system with the main swift services running start the swift-ring-master-server in a shell. It should report that there
    are no required changes:

   fhines@ubuntu:~/ring-master (master)$ ./bin/swift-ring-master-server -f /etc/swift/ring-master.conf  
    ringmasterd Ring-Master starting up
    ringmasterd -> Entering ring orchestration loop.
    ringmasterd ===============================================================================
    ringmasterd Checking on account ring...
    ringmasterd ===============================================================================
    ringmasterd [account] -> No ring change required
    ringmasterd ===============================================================================
    ringmasterd Checking on object ring...
    ringmasterd ===============================================================================
    ringmasterd [object] -> No ring change required
    ringmasterd ===============================================================================
    ringmasterd Checking on container ring...
    ringmasterd ===============================================================================
    ringmasterd [container] -> No ring change required

2. Adjust the target weight of a device in your ring, for example. If all of your devices currently have a weight of 100 try changing one to 80:

    from swift.common.ring import RingBuilder
    import cPickle as pickle
    builder = RingBuilder.load('/etc/swift/object.builder')
    builder.devs[0]['target_weight'] = 80.0 # this is the magic number
    builder._last_part_moves_epoch = builder._last_part_moves_epoch - 86400
    pickle.dump(builder.to_dict(), open('/etc/swift/object.builder', 'wb'), protocol=2)

3. If you start swift-ring-master-server back up (or if you kept it running) you should now see it adjust the ring for you:

    fhines@ubuntu:~/ring-master (master)$ ./bin/swift-ring-master-server -f /etc/swift/ring-master.conf 
    ringmasterd Ring-Master starting up
    ringmasterd -> Entering ring orchestration loop.
    ringmasterd ===============================================================================
    ringmasterd Checking on account ring...
    ringmasterd ===============================================================================
    ringmasterd [account] -> No ring change required
    ringmasterd ===============================================================================
    ringmasterd Checking on object ring...
    ringmasterd ===============================================================================
    ringmasterd --> [127.0.0.1/sdb1] weight 100.0 | target 80.0
    ringmasterd [object] -> ring requires weight change.
    ringmasterd --> Ring last modified 678 seconds ago.
    ringmasterd [object] -> Ring last modify time: ok
    ringmasterd --> Running object dispersion report
    ringmasterd --> Dispersion info: {'object': {'retries': 0, 'copies_expected': 7863, 'missing_2': 0, 'missing_3': 0, 'pct_found': 100.0, 'overlapping': 0, 'copies_found': 7863, 'missing_1': 0}}
    ringmasterd [object] -> Dispersion report: ok
    ringmasterd --> Current balance: 0.00
    ringmasterd [object] -> Current Ring balance: ok
    ringmasterd [object] -> Adjusting ring...
    ringmasterd --> [127.0.0.1/sdb1] -- weight to 90.0
    ringmasterd [object] -> Rebalancing ring...
    ringmasterd --> Reassigned 15124 (5.77%) partitions. Balance is 0.01.
    ringmasterd [object] -> Rebalance: ok
    ringmasterd [object] -> Writing builder...
    ringmasterd --> Backed up /etc/swift/object.builder to /etc/swift/backups/1357628890.object.builder (d4f3edc9d9a53de12ae1e81ff63a9068)
    ringmasterd [object] --> Wrote new builder with md5: e2de1f62c8e0da441b8ef03609127c5f
    ringmasterd [object] -> Writing ring...
    ringmasterd --> Backed up /etc/swift/object.ring.gz to /etc/swift/backups/1357628893.object.ring.gz (c76b64fb7ae4999c846be0635e03708c)
    ringmasterd [object] --> Wrote new ring with md5: a3b27646357f4bd2096625f74640b572
    ringmasterd ===============================================================================
    ringmasterd Checking on container ring...
    ringmasterd ===============================================================================
    ringmasterd [container] -> No ring change required

If you have replication turned on in your SAIO and continue to run swift-ring-master-server you'll see it rebalance/decrease the weight and update the rings until the device matches its target weight.

4. The wsgi app/server just has a single end point of /ring/<ringtype>.tar.gz and supports only a HEAD or GET to that resource (but does support use of a If-None-Match header):

    fhines@kira:~$ http HEAD http://swiftvm.ronin.io:8090/ring/object.ring.gz
    HTTP/1.1 200 OK
    Content-Length: 0
    Content-Type: application/octet-stream
    Date: Tue, 08 Jan 2013 07:25:51 GMT
    Etag: a3b27646357f4bd2096625f74640b572

    fhines@kira:~$ http GET http://swiftvm.ronin.io:8090/ring/object.ring.gz 'If-None-Match:a3b27646357f4bd2096625f74640b572'
    HTTP/1.1 304 Not Modified
    Content-Length: 14
    Content-Type: application/octet-stream
    Date: Tue, 08 Jan 2013 07:34:00 GMT

5. The included swift-ring-minion-server is a simple client that can utilize the swift-ring-master-wsgi app, it can run in either daemon mode or just once via the --once flag. It simply checks the md5sum of the local current copies of its rings and then compares them with what the swift-ring-master-wsgi app reports. If an md5sum differs the client will retrieve the update ring:

    fhines@ubuntu:~/ring-master (master)$ ./bin/swift-ring-minion-server -f /etc/swift/ring-minion.conf --once
    account ring remains unchanged
    object ring updated
    container ring remains unchanged

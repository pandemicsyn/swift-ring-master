swift-ring-master
=================

The swift-ring-master-server is a simple daemon that automates ring and ring
builder change. When it see's that a devices weight does not equal its "target" 
weight it will incrementally adjust, rebalance, and deploy the ring until the 
device(s) reach their target weight.

It attempts to make builder/ring changes in a "safe" fashion by taking
into account:

- Observing the min_part_hours of the ring
- Providing a grace period for builder changes 
- Checking/adjusting the current balance of a ring before making device changes
- Checking dispersion reports to verify cluster health
- ~~Verifying out of place partition percentages across the cluster~~ [no longer present in this version]
- Only adjusting the device(s) by user defined (de|inc)rements

swift-ring-master-wsgi
======================

The optional wsgi app provides a simple interface to remotely checking for changes and retrieving rings:

    fhines@kira:~$ md5sum /etc/swift/object.ring.gz 
    3e1fed98b0ad57d4bc5c17376ef25920  /etc/swift/object.ring.gz

    fhines@kira:~$ http GET http://swiftvm.ronin.io:8090/ring/object.ring.gz 'If-None-Match:3e1fed98b0ad57d4bc5c17376ef25920'
    HTTP/1.1 304 Not Modified
    Content-Length: 14
    Content-Type: application/octet-stream
    Date: Thu, 13 Dec 2012 05:15:46 GMT

    fhines@kira:~$ http HEAD http://swiftvm.ronin.io:8090/ring/object.ring.gz
    HTTP/1.1 200 OK
    Content-Length: 0
    Content-Type: application/octet-stream
    Date: Thu, 13 Dec 2012 05:14:23 GMT
    Etag: 3e1fed98b0ad57d4bc5c17376ef25920

swift-ring-minion
=================

The swift-ring-minion-server is a simple daemon that should be run cluster
wide on all hosts that utilize/require Swift rings. It monitors a
swift-ring-master-server's wsgi app and will automatically retrieve new rings
and deploy them to the local host. It can be run either as a daemon or on demand
(or by cron) via the --once flag:

    fhines@kira:~/ring-master (master)$ bin/swift-ring-minion-server start -f -o
    account ring remains unchanged
    object ring remains unchanged
    container ring remains unchanged

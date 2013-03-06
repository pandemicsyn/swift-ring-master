# swift-ring-master - [![Build Status](https://travis-ci.org/pandemicsyn/swift-ring-master.png?branch=master)](https://travis-ci.org/pandemicsyn/swift-ring-master)

swift-target-weight
===================

The swift-traget-weight cli is a simple utility that lets you list or change/set the
target weight of the devices in the ring:

    fhines@ubuntu:~$ swift-target-weight -h
    Usage:
        usage: swift-target-weight [-l][-t tgt_weight][-s search_pattern] builder_file

        ex: swift-target-weight -t 50 -s 127.0.0.5 /etc/swift/object.builder


    Options:
      -h, --help            show this help message and exit
      -t TARGET_WEIGHT, --target-weight=TARGET_WEIGHT
                            Set given target weight on devices
      -l, --list            List target weight of devices
      -s SEARCH, --search=SEARCH
                            swift-ring-builder compatible search pattern to use
      --swiftdir=SWIFTDIR   Default = /etc/swift

The specified search pattern is compatible with swift-ring-builder's search format:

    The <search-pattern> can be of the form:
        d<device_id>z<zone>-<ip>:<port>/<device_name>_<meta>
        Any part is optional, but you must include at least one part.
        Examples:
            d74              Matches the device id 74
            z1               Matches devices in zone 1
            z1-1.2.3.4       Matches devices in zone 1 with the ip 1.2.3.4
            1.2.3.4          Matches devices in any zone with the ip 1.2.3.4
            z1:5678          Matches devices in zone 1 using port 5678
            :5678            Matches devices that use port 5678
            /sdb1            Matches devices with the device name sdb1
            _shiny           Matches devices with shiny in the meta data
            _"snet: 5.6.7.8" Matches devices with snet: 5.6.7.8 in the meta data
            [::1]            Matches devices in any zone with the ip ::1
            z1-[::1]:5678    Matches devices in zone 1 with ip ::1 and port 5678
        Most specific example:
            d74z1-1.2.3.4:5678/sdb1_"snet: 5.6.7.8"
        Nerd explanation:
            All items require their single character prefix except the ip, in which
            case the - is optional unless the device id or zone is also included.

swift-ring-master-server
========================

The swift-ring-master-server is a simple daemon that automates ring and ring
builder change. It handles the removal or addition of devices as well as weight changes. 
For example, when it see's that a devices weight does not equal its "target" weight it 
will incrementally adjust, rebalance, and deploy the ring until the device(s) reach their
target weight (performing multiple rebalances if needed).

It attempts to make builder/ring changes in a "safe" fashion by taking
into account:

- Observing the min_part_hours of the ring
- Providing a grace period for builder changes 
- Checking/adjusting the current balance of a ring before making device changes
- Checking dispersion reports to verify cluster health
- ~~Verifying out of place partition percentages across the cluster~~ [no longer present in this version]
- Only adjusting the device(s) by user defined (de|inc)rements
- Ability to pause/resume activity

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

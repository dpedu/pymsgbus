pymsgbus
========
**Simple zeromq-based pubsub client and server**


Quick Start
-----------

* Install: `python3 setup.py install`
* Run server: `mbusd -p 7100`
* Connect a peer server: `mbusd -p 7200 --peer 127.0.0.1:7100`
* Dump events: `mbussub -p 7100 --channel orderbook`
* Send events: `mbuspub -p 7200 -c orderbook -m foo bar baz`


About
-----

Pymsgbus is server/client software for passing messages in a
[pub-sub](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern) pattern. It is built on top of
[pyzmq](http://pyzmq.readthedocs.io/en/latest/). This means you can use it with the native pymsgbus client or raw zeromq
sockets. See `msgbus/pub.py` and `msgbus/sub.py` for examples.


Examples
--------

Given the two peered servers created in Quick Start above, and these imports:


```python
from contextlib import closing
from msgbus.client import MsgbusSubClient
```

This code would print out messages from the "orderbook" channel:

```python
with closing(MsgbusSubClient(host, port)) as client:
    client.sub("orderbook")
    while True:
        channel, message = client.recv()
        print("On channel '{}' I got '{}'".format(channel, message))
```

And this code would send a message on the orderbook channel:

```python
with closing(MsgbusSubClient(host, port)) as client:
    for message in messages:
        client.pub("orderbook", "hello world")
```

Because pymsgbus is built on [ZeroMQ](http://zeromq.org/), you don't have to start the clients or servers in any
specific order. You can start the lister client above and then the servers, and ZeroMQ will handle sorting things out.
Of course, messages published when there's no server or no listeners are silently list.


TODO
----

* Remove many 127.0.0.1 hardcodings
* Improve performance
* Create docs

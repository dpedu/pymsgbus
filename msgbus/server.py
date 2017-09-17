import sys
import asyncio
from time import time
import zmq.asyncio
from contextlib import closing
from random import randint
import signal
zmq.asyncio.install()


def exewrap(func):
    async def wrapped(*args, **kwargs):
        try:
            await func(*args, **kwargs)
        except:
            print("EXCEPTIN")
            import traceback
            traceback.print_exc()
    return wrapped


class MsgBusServerPeer(object):

    __slots__ = ["alive", "server", "name", "pub_port", "sub_port", "bind", "protocol", "sub_sock", "sub_sock_addr",
                 "pub_sock", "pub_sock_addr", "last_keepalive", "confpeer", "host"]

    """
    A link to another msgbus server that we share messages with and get messages from.

    Peering process:
    - Another node sends, to our sub port, a __peer_request message with a server name
    - We send a __peer_response message with two port numbers
    - We start listening pub/sub on those two ports
    - Anything we receive goes out to all our clients and peers EXCEPT this one
    """
    def __init__(self, server, name, host, pub_port, sub_port, bind=True):
        self.alive = False
        self.server = server
        self.name = name
        self.host = host
        self.pub_port = pub_port
        self.sub_port = sub_port
        self.bind = bind

        self.protocol = 'tcp'
        self.sub_sock = None
        self.sub_sock_addr = '{}://{}:{}'.format(self.protocol, self.host, self.sub_port)
        self.pub_sock = None
        self.pub_sock_addr = '{}://{}:{}'.format(self.protocol, self.host, self.pub_port)

        self.last_keepalive = time()

        self.confpeer = None

        print("New peer on {}, {}".format(self.pub_port, self.sub_port))

    async def run(self):
        # Wait for messages
        # On recv, pass it to the server for transmission to all peers but the sender
        with closing(self.server.ctx.socket(zmq.SUB)) as self.sub_sock:
            with closing(self.server.ctx.socket(zmq.PUB)) as self.pub_sock:
                if self.bind:
                    self.pub_sock.bind(self.pub_sock_addr)
                    self.sub_sock.bind(self.sub_sock_addr)
                else:
                    self.pub_sock.connect(self.pub_sock_addr)
                    self.sub_sock.connect(self.sub_sock_addr)
                self.sub_sock.subscribe(b'')
                self.alive = True
                await asyncio.wait([self.listen(),
                                    self.keep_alive_listener(),
                                    self.keep_alive_sender()], loop=self.server.loop)

    async def listen(self):
        while self.alive:
            # __peer_msg <origin> <topic> <data>
            # print("peer recv")
            msg = await self.sub_sock.recv()
            topic, msg = msg.decode('utf-8').split(' ', 1)
            # print('peer received topic:"{}" data:"{}"'.format(topic, msg))
            if topic == "__peer_msg":
                sender, data = msg.split(' ', 1)
                self.server.counter_remote_messages += 1
                await asyncio.wait([self.server.pub_sock.send(data.encode("utf-8")),
                                    self.server.send_to_peers(data, exclude=sender)],
                                   loop=self.server.loop)
                # print("Sent!")
            elif topic == "__peer_keepalive":
                self.last_keepalive = time()

    async def local_send(self, msg):
        # Send a raw message to peers
        if self.alive:
            await self.pub_sock.send('__peer_msg {} {}'.format(self.server.name, msg).encode('utf-8'))

    async def keep_alive_sender(self):
        interval = self.server.conf.get("peer_keepalive_interval", 1)
        try:
            while self.alive:
                await self.pub_sock.send("__peer_keepalive ping".encode('utf-8'))
                await asyncio.sleep(interval)
        except:
            import traceback
            traceback.print_exc()

    async def keep_alive_listener(self):
        interval = self.server.conf.get("peer_keepalive_timeout", 5)
        try:
            while self.alive:
                if time() - self.last_keepalive > interval:
                    print("Peer {} is lost!".format(self.name))
                    self.server.disconnect_peer(self.name)
                    break
                await asyncio.sleep(1)
        except:
            import traceback
            traceback.print_exc()

    def shutdown(self):
        self.alive = False
        self.sub_sock.close()
        self.pub_sock.close()


class MsgBusServer(object):

    __slots__ = ["alive", "loop", "ctx", "subport", "pubport", "protocol", "sub_sock", "sub_sock_addr", "pub_sock",
                 "pub_sock_addr", "seed_peers", "peers", "name", "port_range", "counter_local_messages",
                 "counter_remote_messages", "conf", "inprogress_connects", "bind_host"]

    def __init__(self, loop, ctx, bind_host, pubport, subport, port_range, peers):
        assert subport != pubport
        self.alive = True  # TODO move this?

        self.loop = loop
        self.ctx = ctx
        self.bind_host = bind_host
        self.subport = subport
        self.pubport = pubport

        self.protocol = 'tcp'
        self.sub_sock = None
        self.sub_sock_addr = '{}://{}:{}'.format(self.protocol, self.bind_host, self.subport)
        self.pub_sock = None
        self.pub_sock_addr = '{}://{}:{}'.format(self.protocol, self.bind_host, self.pubport)

        self.seed_peers = peers
        self.peers = {}

        self.name = "randomname_{}".format(randint(0, 420000))

        self.port_range = port_range

        self.counter_local_messages = 0
        self.counter_remote_messages = 0

        self.conf = {}

        self.inprogress_connects = []

    async def run(self):
        with closing(self.ctx.socket(zmq.SUB)) as self.sub_sock:
            with closing(self.ctx.socket(zmq.PUB)) as self.pub_sock:

                self.sub_sock.bind(self.sub_sock_addr)
                self.sub_sock.subscribe(b'')
                self.pub_sock.bind(self.pub_sock_addr)
                await asyncio.wait([self.reciever(),
                                    self.heartbeat(),
                                    self.peer_monitor(),
                                    self.stats_monitor()], loop=self.loop)

    async def stats_monitor(self):
        """
        Print out stats on an interval (messages/s etc)
        """
        start = time()
        interval = self.conf.get("stats_interval", 5)
        show_idle_stats = self.conf.get("show_idle_stats", True)
        counter_local_total = 0
        counter_remote_total = 0
        while self.alive:
            await asyncio.sleep(interval)
            if show_idle_stats or self.counter_local_messages > 0 or self.counter_remote_messages > 0:
                counter_local_total += self.counter_local_messages
                counter_remote_total += self.counter_remote_messages
                print("Last {}s i delivered {} messages locally ({}/s)"
                      .format(interval, self.counter_local_messages, round(self.counter_local_messages / interval, 2)))
                print("Last {}s i delivered {} messages remotely ({}/s)"
                      .format(interval, self.counter_remote_messages, round(self.counter_remote_messages / interval, 2)))
                total = self.counter_local_messages + self.counter_remote_messages
                print("Last {}s total {} messages ({}/s)".format(interval, total, round(total / interval, 2)))
                uptime = round(time() - start, 2)
                print("Lifetime {}s i delivered {} messages locally ({}/s)"
                      .format(uptime, counter_local_total, round(counter_local_total / uptime, 2)))
                print("Lifetime {}s i delivered {} messages remotely ({}/s)"
                      .format(uptime, counter_remote_total, round(counter_remote_total / uptime, 2)))
                total = counter_local_total + counter_remote_total
                print("Lifetime {}s total {} messages ({}/s)\n".format(uptime, total, round(total / uptime, 2)))
            self.counter_local_messages = 0
            self.counter_remote_messages = 0

    @exewrap
    async def peer_monitor(self):
        # ensure we stay connected to peers
        while self.alive:
            for peer_addr in self.seed_peers:
                if not self.has_peer(peer_addr):
                    print("connecting to {}".format(peer_addr))
                    self.loop.call_soon(asyncio.ensure_future, self.connect_to_peer(peer_addr))
            await asyncio.sleep(1)

    def has_peer(self, peer_name):
        if peer_name in self.inprogress_connects:
            return True
        if peer_name in [i.confpeer for k, i in self.peers.items()]:
            return True
        return False

    @exewrap
    async def connect_to_peer(self, peer_name):
        try:
            self.inprogress_connects.append(peer_name)
            host, pub_port = peer_name.split(":")
            peer_pub_addr = '{}://{}:{}'.format(self.protocol, host, pub_port)
            with closing(self.ctx.socket(zmq.SUB)) as peer_pub_socket:
                peer_pub_socket.connect(peer_pub_addr)
                peer_pub_socket.subscribe(b'__msgbus_meta')
                async def wait_for_cmd(cmd_name, timeout=10):
                    start = time()
                    while time() - start < timeout:
                        try:
                            msg = await peer_pub_socket.recv(flags=zmq.NOBLOCK)
                            if msg:
                                _, cmd, data = msg.decode("utf-8").split(" ", 2)  # only subscribed to __msgbus_meta
                                if cmd == cmd_name:
                                    return data
                        except zmq.error.Again:
                            await asyncio.sleep(0.1)

                print("waiting for peer info")
                peer_info = await wait_for_cmd("__my_info")

                if not peer_info:
                    print("peering failed for", peer_name)
                    return

                remote_name, subport, subproto, pubbport, pubproto = peer_info.split()
                peer_sub_addr = '{}://{}:{}'.format(subproto, host, subport)
                peer_response = []
                print(peer_sub_addr)
                with closing(self.ctx.socket(zmq.PUB)) as peer_sub_socket:
                    peer_sub_socket.connect(peer_sub_addr)
                    await asyncio.sleep(1)
                    while self.alive:
                        await peer_sub_socket.send("__msgbus_meta __peer_request {}".format(self.name).encode('utf-8'))
                        peer_response = await wait_for_cmd("__peer_response")
                        if peer_response:
                            print("got peer resp: ", peer_response)
                            break
                        await asyncio.sleep(1)

                if not peer_response:
                    # TODO RETRY
                    print("peering failed for", peer_name)
                    return

                _, pubport, pubproto, subport, subproto = peer_response.split()
                peer = self.new_peer(remote_name, host, pub_port=pubport, sub_port=subport, bind=False)
                peer.confpeer = peer_name
                print("Added peer", peer_name)
        finally:
            self.inprogress_connects.remove(peer_name)

    async def heartbeat(self):
        """
        Send heartbeat messages every second. These messages, all under the __msgbus_meta topic, include:
        * __my_ports: a listing of this nodes pub and sub ports and protocols
        """
        while self.alive:
            msg = '__msgbus_meta __my_info {} {} {} {} {}'.format(self.name, self.subport, self.protocol,
                                                                  self.pubport, self.protocol)
            self.pub_sock.send(msg.encode('utf-8'))
            await asyncio.sleep(1)

    async def reciever(self):
        try:
            while self.alive:
                # print("recv")
                msg = await self.sub_sock.recv()
                msg = msg.decode("utf-8")
                topic, data = msg.split(' ', 1)
                # print('received topic:"{}" data:"{}"'.format(topic, data))
                if topic == "__msgbus_meta":
                    await self.process_meta(data)
                else:
                    await asyncio.wait([self.pub_sock.send(msg.encode("utf-8")), self.send_to_peers(msg)],
                                       loop=self.loop)
                    self.counter_local_messages += 1
        except:
            import traceback
            traceback.print_exc()

    async def process_meta(self, data):
        print("Got meta: {}".format(data))
        command, rest = data.split(" ", 1)
        if command == "__peer_request":
            try:
                await self.handle_peer_request(rest)
            except:
                import traceback
                traceback.print_exc()

    async def handle_peer_request(self, peer_name):
        assert type(peer_name) is str
        peer = self.new_peer(peer_name, self.bind_host)
        await self.pub_sock.send("__msgbus_meta __peer_response {} {} tcp {} tcp"
                                 .format(peer_name, peer.pub_port, peer.sub_port)
                                 .encode("utf-8"))

    def new_peer(self, peer_name, host, pub_port=None, sub_port=None, bind=True):
        if peer_name not in self.peers:
            if pub_port is None:
                pub_port = randint(*self.port_range)
            if sub_port is None:
                sub_port = pub_port + 1
            peer_ports = (sub_port, pub_port)
            self.peers[peer_name] = MsgBusServerPeer(self, peer_name, host, *peer_ports, bind=bind)
            self.loop.call_soon(asyncio.ensure_future, self.peers[peer_name].run())  # seems to act like a fork
        return self.peers[peer_name]

    def disconnect_peer(self, peer_name):
        if peer_name in self.peers:
            peer = self.peers[peer_name]
            peer.shutdown()
            del self.peers[peer_name]

    async def send_to_peers(self, msg, exclude=None):
        for peer_name, peer in self.peers.items():
            if peer_name == exclude:
                continue
            await peer.local_send(msg)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="msgbus server")
    parser.add_argument("-b", "--bind-host", default="0.0.0.0", help="bind host")
    parser.add_argument("-p", "--port", default=7000, help="server publisher port")
    parser.add_argument("-n", "--peers", nargs="+", help="connect to peer's publisher port 1.2.3.4:5678")
    parser.add_argument("-r", "--port-range", default=[7010, 7400], nargs=2, help="peer port range")
    args = parser.parse_args()

    with zmq.asyncio.Context() as ctx:
        loop = asyncio.get_event_loop()
        # loop.set_debug(True)
        server = MsgBusServer(loop, ctx, args.bind_host, int(args.port), int(args.port) + 1,
                              port_range=args.port_range, peers=args.peers if args.peers else [])

        def signal_handler(signum, stack):
            nonlocal loop
            print('Received:', signum)
            server.alive = False
            ctx.destroy()
            sys.exit(signum)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        loop.run_until_complete(server.run())


if __name__ == '__main__':
    main()

"""
More on peering:

./server.py -p 7003  # start a node on port 7003 + 7004
./server.py -p 7200 --peer 127.0.0.1:7004  # connect another node
./server.py -p 7300 --peer 127.0.0.1:7201  # connect a third node, in a chain
./server.py -p 7400 --peer 127.0.0.1:7201  # connect a third node, making 7200 a hub

- This server has 2 ports for all normal clients
- peers use this normal port to request peering. The existing server is the REMOTE, the other is CLIENT
    - CLIENT: subscribe to the configured host and IP, it is a publishing port
    - REMOTE: (periodically) sends and info heartbeat advertising the REMOTE's subscribing port
    - CLIENT: waits for ^ msg, connects to REMOTE's sub port
    - CLIENT: send a __peer_request, indicating our identity (name)
    - REMOTE: (always upon receiving a __peer_request) create a MsgBusServerPeer if one doesnt exist for the requester's
      identity. It would start listening now or already be. Send a __peer_response containing the peer's name and ports.
    - CLIENT: wait for a __peer_response. retry or panic if doesnt come X seconds after the request. Create a
      MsgBusServerPeer object (probably manually checking for dupe this time).

    MsgBusServerPeer:
        - Creates a pub/sub port pair for the peer
        - In parallel:
            - Listen for incoming messages from peers
                - Upon recv, forward to all peers (besides the sender) and local clients
            - When local clients send msgs, send to all peers

"""

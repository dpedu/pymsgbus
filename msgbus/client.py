import zmq
from threading import Semaphore
from time import sleep, time


class PublishSetupException(Exception):
    pass


class MsgbusSubClient(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.ctx = None  # ZMQ context
        self.sub_socket = None  # listener sockets
        self.subscriptions = []
        self.pub_socket = None  # publisher socket
        self.lock = Semaphore(1)
        self.connect()

    def close(self):
        if self.sub_socket:
            self.sub_socket.close()
        if self.pub_socket:
            self.pub_socket.close()
        if self.ctx:
            self.ctx.destroy()

    def connect(self):
        if not self.ctx:
            self.ctx = zmq.Context()
        self.sub_socket = self.ctx.socket(zmq.SUB)
        self.sub_socket.connect("tcp://{}:{}".format(self.host, self.port))

    def sub(self, channel=None):
        if channel is None:
            channel = ''
        assert type(channel) is str
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, channel.encode("utf-8"))
        self.subscriptions.append(channel)

    def unsub(self, channel):
        assert type(channel) is str
        if channel in self.subscriptions:
            self.subscriptions.remove(channel)
            self.sub_socket.setsockopt(zmq.UNSUBSCRIBE, channel.encode("utf-8"))

    def recv(self, decode=True, block=True):
        recv_args = (zmq.NOBLOCK, ) if not block else ()
        message = self.sub_socket.recv(*recv_args)
        channel, body = message.split(b' ', 1)
        return channel.decode("utf-8"), (body.decode('utf-8') if decode else body)

    def _setup_publish_socket(self, timeout=5):
        start = time()
        try:
            self.sub("__msgbus_meta")
            while not timeout or time() < start + timeout:
                try:
                    channel, message = self.recv(block=False)
                except zmq.error.Again:
                    sleep(0.01)
                    continue
                if channel != "__msgbus_meta":
                    continue
                meta, args = message.split(" ", 1)
                if meta != "__my_info":
                    continue
                server_name, subport, subproto, pubbport, pubproto = args.split(" ")
                remote = "tcp://{}:{}".format(self.host, subport)
                pub_socket = self.ctx.socket(zmq.PUB)
                pub_socket.connect(remote)
                return pub_socket
            raise PublishSetupException("Could not establish publisher socket")
        finally:
            self.unsub("__msgbus_meta")

    def pub(self, channel, message, encode_msg=True, settle=True, timeout=5):
        if encode_msg:
            message = message.encode("utf-8")
        if not self.pub_socket:
            with self.lock:
                self.pub_socket = self._setup_publish_socket(timeout)
                if settle:
                    sleep(1)
        self.pub_socket.send(channel.encode("utf-8") + b' ' + message)

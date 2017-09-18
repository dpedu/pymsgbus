import zmq
from threading import Semaphore
from time import sleep, time


class PublishSetupException(Exception):
    pass


class MsgbusSubClient(object):
    def __init__(self, host, port=None, pubport=None):
        self.host = host
        self.port = port
        self.pubport = pubport
        self._ctx = None  # ZMQ context
        self.sub_socket = None  # listener sockets
        self.subscriptions = []
        self.pub_socket = None  # publisher socket
        self.lock = Semaphore(1)
        # self.connect()

    def close(self):
        if self.sub_socket:
            self.sub_socket.close()
        if self.pub_socket:
            self.pub_socket.close()
        if self.ctx:
            self.ctx.destroy()

    @property
    def ctx(self):
        if not self._ctx:
            self._ctx = zmq.Context()
        return self._ctx

    def connect(self):
        if self.port and not self.sub_socket:
            self.connect_sub(self.host, self.port)
        if self.pubport and not self.pub_socket:
            self.connect_pub(self.host, self.pubport)

    def connect_sub(self, host, port):
        self.sub_socket = self.ctx.socket(zmq.SUB)
        self.sub_socket.connect("tcp://{}:{}".format(host, port))

    def connect_pub(self, host, port):
        pub_socket = self.ctx.socket(zmq.PUB)
        pub_socket.connect("tcp://{}:{}".format(host, port))
        self.pub_socket = pub_socket

    def sub(self, channel=None):
        if channel is None:
            channel = ''
        assert type(channel) is str
        if not self.sub_socket:
            self.connect_sub(self.host, self.port)
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
                server_name, subport, subproto, pubport, pubproto = args.split(" ")
                self.pubport = subport
                self.connect_pub(self.host, self.pubport)
                return
            raise PublishSetupException("Could not establish publisher socket")
        finally:
            self.unsub("__msgbus_meta")

    def pub(self, channel, message, encode_msg=True, settle=True, timeout=5):
        if encode_msg:
            message = message.encode("utf-8")
        if not self.pub_socket:
            with self.lock:
                self.prepare_pub(timeout=timeout, settle=settle)
        self.pub_socket.send(channel.encode("utf-8") + b' ' + message)

    def prepare_pub(self, timeout=5, settle=True):
        self._setup_publish_socket(timeout)
        if settle:
            sleep(1)

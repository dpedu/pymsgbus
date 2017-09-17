
def listen_native(host, port, channels):
    """
    Subscribe to the given server/channels using the msgbus client
    """
    from contextlib import closing
    from msgbus.client import MsgbusSubClient
    with closing(MsgbusSubClient(host, port)) as client:
        if channels:
            for channel in channels:
                client.sub(channel)
        else:
            client.sub()
        while True:
            yield "{} {}".format(*client.recv())


def listen_zmq(host, port, channels):
    """
    Example subscribing to the given server/channels using a raw zeromq socket
    """
    import zmq
    with zmq.Context() as ctx:
        forward_socket = ctx.socket(zmq.SUB)
        forward_socket.connect("tcp://{}:{}".format(host, port))
        if channels:
            for channel in channels:
                forward_socket.setsockopt(zmq.SUBSCRIBE, channel.encode("utf-8"))
        else:
            forward_socket.setsockopt(zmq.SUBSCRIBE, b'')
        while True:
            transport = forward_socket.recv()
            yield transport.decode("utf-8")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="dump all messages from msgbus")
    parser.add_argument("-i", "--host", default="127.0.0.1", help="host to connect to")
    parser.add_argument("-p", "--port", type=int, help="port to connect to")
    parser.add_argument("-c", "--channel", nargs="+", help="dump only channels")
    parser.add_argument("--type", default="native", choices=["native", "raw"], help="client type")
    args = parser.parse_args()

    client = None
    if args.type == "native":
        client = listen_native(args.host, args.port, args.channel)
    elif args.type == "raw":
        client = listen_zmq(args.host, args.port, args.channel)

    for line in client:
        print(line)


if __name__ == '__main__':
    main()

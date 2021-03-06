
def send_native(host, port, pubport, channel, messages):
    """
    Send some messages on a specified channel using the msgbus client. Note that if we don't specify a non-None
    publishing port when creating the client, which means the client will find it using metadata the server publishes
    on `port`.
    """
    from contextlib import closing
    from msgbus.client import MsgbusSubClient
    with closing(MsgbusSubClient(host, port)) as client:
        for message in messages:
            client.pub(channel, message)


def send_zmq(host, port, channel, messages):
    """
    Send some messages on a specified channel using a raw zmq socket.
    Note: the native client connects to the server's publisher port and autodetects the server's subscriber port. The
    raw zmq socket must connect directly to the subscriber port. The first pub() of the native client will block as it
    listens for metadata from the server to establish the underlying zmq publisher socket. if the server is down, the
    native client could raise PublishSetupException or block as long as the timeout. The raw zmq socket is allowed to
    silently fail, in this example.
    """
    import zmq
    from time import sleep
    with zmq.Context() as ctx:
        forward_socket = ctx.socket(zmq.PUB)
        forward_socket.connect("tcp://{}:{}".format(host, port))
        sleep(1)
        for message in messages:
            m = "{} {}".format(channel, message).encode("utf-8")
            print(m)
            forward_socket.send(m)
        sleep(1)
        forward_socket.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="send a message to a msgbus server")
    parser.add_argument("-i", "--host", default="127.0.0.1", help="host to connect to")
    parser.add_argument("-p", "--port", default=7003, help="port to connect to")
    parser.add_argument("-c", "--channel", required=True, help="message channel")
    parser.add_argument("-m", "--message", required=True, nargs="+", help="message bodies")
    parser.add_argument("--type", default="native", choices=["native", "raw"], help="client type")
    args = parser.parse_args()

    if args.type == "native":
        send_native(args.host, args.port, None, args.channel, args.message)
    elif args.type == "raw":
        send_zmq(args.host, args.port, args.channel, args.message)


if __name__ == '__main__':
    main()

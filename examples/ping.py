from contextlib import closing
from msgbus.client import MsgbusSubClient
from time import time, sleep


def pong(host, port):
    """
    Subscribe to the ping channel and send pongs in reply
    """
    with closing(MsgbusSubClient(host, port)) as client:
        client.sub("ping")
        while True:
            _, msg = client.recv()
            client.pub("pong", msg)
            print("pong(): >< {} {}".format(_, msg))


def ping(host, port, message, count=5, interval=1):
    """
    Send a ping and wait for the reply. In a loop
    """
    with closing(MsgbusSubClient(host, port)) as client:
        client.sub("pong")
        sleep(1)

        while count > 0:
            count -= 1
            print("ping(): > ping", message)
            start = time()
            client.pub("ping", message)
            _, msg = client.recv()
            print("ping(): <", _, msg, "rtt:", round(time() - start, 6), "\n")
            sleep(interval)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="send a message to a msgbus server")
    parser.add_argument("-i", "--host", default="127.0.0.1", help="host to connect to")
    parser.add_argument("-p", "--port", default=7003, help="port to connect to")
    parser.add_argument("-m", "--mode", choices=["ping", "pong"], required=True, help="client mode")
    parser.add_argument("--payload", default="hello", help="ping payload")
    parser.add_argument("-c", "--count", type=int, default=5, help="how many pings")
    parser.add_argument("--interval", type=float, default=1, help="ping interval")
    args = parser.parse_args()

    if args.mode == "ping":
        ping(args.host, args.port, args.payload, args.count, args.interval)
    elif args.mode == "pong":
        pong(args.host, args.port)

    # python3 examples/ping.py -m pong -i 127.0.0.1 -p 7100 &
    # python3 examples/ping.py -m ping -i 127.0.0.1 -p 7100
    # ping(): > ping hello
    # pong(): >< ping hello
    # ping(): < pong hello rtt: 0.000666


if __name__ == '__main__':
    main()

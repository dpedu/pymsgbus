import asyncio
import zmq.asyncio
from time import time
from contextlib import closing
# from concurrent.futures import ThreadPoolExecutor
from random import randint
zmq.asyncio.install()


async def perftest_sender(ctx, port, num_messages, data):
    with closing(ctx.socket(zmq.PUB)) as pub_sock:
        pub_sock.connect("tcp://127.0.0.1:{}".format(port))
        await asyncio.sleep(5)
        start = time()
        total = num_messages
        while total > 0:
            total -= 1
            await pub_sock.send(data)
            if total % 1000 == 0:
                print("to send:", total)
        duration = round(time() - start, 6)
        print("Send {} messages in {}s".format(num_messages, duration))
        print("{} m/s!".format(round(num_messages / duration, 2)))


async def perftest_recver(ctx, port, num_messages, topic):
    try:
        with closing(ctx.socket(zmq.SUB)) as sub_sock:
            sub_sock.connect("tcp://127.0.0.1:{}".format(port))
            sub_sock.subscribe(topic)

            start = None
            total = num_messages
            recv_bytes = 0
            print("recver waiting")
            while total > 0:
                message = await sub_sock.recv()
                recv_bytes += len(message)
                if not start:  # start timer when we get the first message
                    start = time()
                if total % 1000 == 0:
                    print("to recv:", total)
                total -= 1
            duration = round(time() - start, 6)
            print("Recv'd {} kbytes in {} messages in {}s".format(round(recv_bytes / 1000), num_messages, duration))
            print("{} m/s!".format(round(num_messages / duration, 2)))
    except:
        import traceback
        traceback.print_exc()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="msgbus performance test")
    parser.add_argument("-p", "--send-port", default=7003, help="port to send to")
    parser.add_argument("-r", "--recv-port", default=7004, help="port to recv to")
    parser.add_argument("-m", "--messages", type=int, default=10000, help="number of messages to send")
    parser.add_argument("-t", "--topic", default="perftest")
    parser.add_argument("--message", default="message", help="content to send")
    args = parser.parse_args()

    with zmq.asyncio.Context() as ctx:
        loop = asyncio.get_event_loop()
        # loop.set_default_executor(ThreadPoolExecutor(max_workers=10))
        loop.set_debug(True)
        message = "{} {}".format(args.topic, args.message).encode("utf-8")
        topic = args.topic.encode("utf-8")
        loop.run_until_complete(asyncio.wait([perftest_recver(ctx, args.recv_port, args.messages, topic),
                                              perftest_sender(ctx, args.send_port, args.messages, message)]))


if __name__ == '__main__':
    main()



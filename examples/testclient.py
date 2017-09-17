from msgbus.client import MsgbusSubClient


def main():
    c = MsgbusSubClient("127.0.0.1", 7200)

    c.sub("orderbook")

    print(c.recv())  # returns (channel, message)

    c.pub("hello_world", "asdf1234")
    c.pub("hello_world", "qwer5678")


if __name__ == '__main__':
    main()

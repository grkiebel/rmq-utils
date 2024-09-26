from time import sleep
from typing import Tuple
from rmq_utils import (
    Parameters,
    Broker,
    Connections,
    Sender,
    Receiver,
    Consumer,
    Runner,
)


def get_msg_callback(name: str):
    def msg_callback(msg: Tuple[str, str]):
        key, payload = msg
        print(
            f"👉 Reciever {name}: Received message with key {key} and payload {payload}"
        )

    return msg_callback


def post_messages(sender: Sender, keys: list[str]):
    for i in range(5):
        for key in keys:
            post_message(sender, key, f"Message {i}")


def post_message(sender, key, payload):
    sender.post_message(sender.message_tuple(key, payload))
    sleep(0.2)


def simple_example():
    """
    This example demonstrates how to use the rmq_utils package to access the
    message broker with a sender and a receiver.  The sender sends messages to the
    exchange, and the receiver receives messages using a fanout routing exchange.
    """
    Parameters.set(log_level="WARNING")
    # Note: Other properties can be set on Parameters as well.

    exchange_name = "bozo"
    Broker.declare_exchange("main", exchange_name)
    Connections.close()

    sender = Sender("SND", exchange_name)
    receiver = Receiver("REC", exchange_name)
    consumer = Consumer("CNS", receiver.get_queue(), get_msg_callback("REC"))

    sender.start()
    receiver.start()
    consumer.start()

    post_message(sender, "REC", "Hello, World!")

    print("✋🏻 Stopping...")
    sender.stop()
    receiver.stop()
    consumer.stop()

    Broker.delete_exchange(exchange_name)

    print("✅ Done!")


def more_complicated_example():
    """
    This example demonstrates how to use the rmq_utils package to create a simple
    message broker with a sender and two receivers.  The sender sends messages to
    the exchange, and the receivers receive messages using a direct routing exchange.
    A runner object is used to simplify the starting and stopping of multiple
    components.
    """
    Parameters.set(log_level="WARNING")
    # Note: Other properties can be set on Parameters as well.

    exchange_name = "bozo"
    Broker.declare_exchange("main", exchange_name, type="direct")
    Connections.close()

    runner = Runner()

    sender = Sender("SND", exchange_name)
    runner += sender

    receiver_names = ["REC-1", "REC-2"]
    for name in receiver_names:
        receiver = Receiver(name, exchange_name)
        consumer = Consumer(name, receiver.get_queue(), get_msg_callback(name))
        runner += receiver
        runner += consumer

    runner.start_all()

    post_messages(sender, receiver_names)

    print("✋🏻 Stopping...")
    runner.stop_all()

    Broker.delete_exchange(exchange_name)

    print("✅ Done!")


def main():
    simple_example()
    more_complicated_example()


if __name__ == "__main__":

    main()

from time import sleep
from rmq_utils import (
    Parameters,
    Broker,
    Connections,
    Message,
    Sender,
    Receiver,
    Consumer,
    Runner,
)


def main():
    """
    This example demonstrates how to use the rmq_utils package to create a simple
    message broker with a sender and two receivers.  The sender sends messages to
    the exchange, and the receivers receive messages from the exchange using
    direct routing.
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

    print("❎ Stopping...")
    runner.stop_all()

    Broker.delete_exchange(exchange_name)


def get_msg_callback(name: str):
    def msg_callback(msg: Message):
        print(
            f"👉 Reciever {name}: Received message with key {msg.key} and payload {msg.payload}"
        )

    return msg_callback


def post_messages(sender: Sender, keys: list[str]):
    for i in range(5):
        for key in keys:
            sender.post_message(Message(key=key, payload=f"Message {i}"))
            sleep(0.2)


if __name__ == "__main__":

    main()

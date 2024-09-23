import threading
import queue
import pika
from pika import channel as PChan
from pydantic import BaseModel
from rmq_utils.src.connections import Connections
from rmq_utils.src.params import Parameters
from rmq_utils.src.broker import Broker
from rmq_utils.src.thread_mgr import ThreadManager
from rmq_utils.src.log_config import get_logger


logger = get_logger(__name__)


class Message(BaseModel):
    """Defines form of messages to be sent to or received from the message broker."""

    key: str
    payload: str


class PubSubBase(ThreadManager):

    def __init__(self, name: str, exchange_name: str):
        super().__init__(name)
        self.ex_name = exchange_name

    def publish_message(self, ex_name: str, msg: Message):
        """Send a message to the exchange on the message broker."""
        channel = Connections.get_channel()
        channel.basic_publish(exchange=ex_name, routing_key=msg.key, body=msg.payload)
        logger.info(f"Sender {self.name}: Sent message to {ex_name} with key {msg.key}")


class Sender(PubSubBase):
    """
    Implements a publisher that sends messages to a RabbitMQ message broker.
    Messages to be sent are posted to a local queue by clients.
    """

    def __init__(self, name: str, exchange_name: str):
        super().__init__(name, exchange_name)

    def _main_loop(self):
        """
        Look for messages in the local queue and send them to the message broker.
        If no message is available, timeout and check the stop flag.  If the stop
        flag is set, stop the thread.
        (runs in a separate thread)
        """
        logger.info(f"Sender {self.name}: is running")
        self.started.set()
        while True:
            if self.stop_flag.is_set():
                break
            if not (msg := self.get_msg_from_local_queue()):
                continue
            self.publish_message(self.ex_name, msg)
        Connections.close()

    def post_message(self, msg: Message):
        self._local_queue.put(msg)


class Receiver(PubSubBase):
    """
    Implements a subscriber that listens to a message queue for incoming messages
    with a timeout.  If a message is available, it is sent to client. If timeout
    occurs, the thread checks the stop flag and stops listening if it is set.
    """

    def __init__(self, name: str, exchange_name: str):
        super().__init__(name, exchange_name)

    def _main_loop(self):
        """Start the subscriber thread."""
        logger.info(f"Receiver {self.name} is running")
        q_name = f"Q_{self.name}"
        Broker.declare_queue(q_name, self.ex_name, self.name)
        channel: PChan = Connections.get_channel()
        channel.start_consuming()
        self.started.set()
        try:
            for method, properties, body in channel.consume(
                q_name, inactivity_timeout=Parameters.loop_interval
            ):
                if method:
                    channel.basic_ack(method.delivery_tag)
                    msg = Message(key=method.routing_key, payload=body.decode())
                    self._local_queue.put(msg)
                    logger.info(f"{self.name}: received message")
                    continue

                if self.stop_flag.is_set():
                    channel.cancel()
                    logger.info(f"Receiver {self.name}: Stopping...")
                    Connections.close()
                    break

        except pika.exceptions.AMQPConnectionError as e:
            logger.exception(f"Consumer connection error: {e}")
            if self.my_thread and self.my_thread.is_alive():
                self._main_loop()
            else:
                self.start()

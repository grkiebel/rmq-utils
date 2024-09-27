from typing import Tuple
import pika
from pika import channel as PChan
from rmq_utils.src.connections import Connections
from rmq_utils.src.params import Parameters
from rmq_utils.src.broker import Broker
from rmq_utils.src.thread_mgr import ThreadManager
from rmq_utils.src.log_config import get_logger


logger = get_logger(__name__)


class PubSubBase(ThreadManager):

    def __init__(self, name: str, exchange_name: str):
        super().__init__(name)
        self.ex_name = exchange_name

    def publish_message(self, msg: Tuple[str, str]):
        """Send a message to the exchange on the message broker."""
        key, payload = msg
        channel = Connections.get_channel()
        channel.basic_publish(exchange=self.ex_name, routing_key=key, body=payload)
        logger.info(
            f"Sender {self.name}: Sent message to {self.ex_name} with key {key}"
        )

    def message_tuple(self, key: str, payload: str) -> Tuple[str, str]:
        return key, payload


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
            self.publish_message(msg)
        Connections.close()

    def post_message(self, msg: Tuple[str, str]):
        self._local_queue.put(msg)


class Receiver(PubSubBase):
    """
    Implements a subscriber that listens to an AMQP message broker for incoming messages
    and forwards them to a local queue.
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
                    msg = self.message_tuple(method.routing_key, body.decode())
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

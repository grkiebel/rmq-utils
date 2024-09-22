from pika import channel as PChan
from rmq_utils.src.connections import Connections
from rmq_utils.src.log_config import get_logger

logger = get_logger(__name__)


class Broker:

    @staticmethod
    def declare_exchange(name: str, ex_name: str, type: str = "fanout"):
        """Declare an exchange on the message broker."""
        channel: PChan = Connections.get_channel()
        channel.exchange_declare(exchange=ex_name, exchange_type=type)
        logger.info(f"{name}: Declared exchange {ex_name}")

    @staticmethod
    def delete_exchange(ex_name: str):
        """Delete an exchange on the message broker."""
        channel: PChan = Connections.get_channel()
        channel.exchange_delete(exchange=ex_name)
        logger.info(f"Deleted exchange {ex_name}")

    @staticmethod
    def declare_queue(q_name: str, ex_name: str, name: str):
        """Declare a queue on the message broker and bind it to the exchange."""
        channel: PChan = Connections.get_channel()
        channel.queue_declare(queue=q_name, auto_delete=True)
        channel.queue_bind(exchange=ex_name, queue=q_name, routing_key=name)
        logger.info(
            f"{name}: Queue {q_name} declared on exchange {ex_name} with binding key {name}"
        )

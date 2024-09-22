import pika
from pika import channel as PChan, connection as PConn
import threading
from .log_config import get_logger
from .params import Parameters


logger = get_logger(__name__)

# TODO: prune closed connections and channels


class Connections:
    _connections = {}
    _channels = {}
    _lock = threading.RLock()

    def __new__(cls, *args, **kwargs):
        raise TypeError(f"Instances of {cls.__name__} are prohibited.")

    @classmethod
    def get_connection(cls) -> PConn:
        logger.setLevel(Parameters.log_level)
        thread_id = threading.get_ident()
        with cls._lock:
            if (
                thread_id not in cls._connections
                or cls._connections[thread_id].is_closed
            ):
                cls._connections[thread_id] = pika.BlockingConnection(
                    Parameters.connection()
                )
                logger.info(f"Thread {thread_id}: Creating a new connection.")
            return cls._connections[thread_id]

    @classmethod
    def get_channel(cls) -> PChan:
        logger.setLevel(Parameters.log_level)
        thread_id = threading.get_ident()
        with cls._lock:
            if thread_id not in cls._channels or cls._channels[thread_id].is_closed:
                connection = cls.get_connection()
                cls._channels[thread_id] = connection.channel()
                logger.info(f"Thread {thread_id}: Creating a new channel.")
            return cls._channels[thread_id]

    @classmethod
    def close_connection(cls):
        logger.setLevel(Parameters.log_level)
        thread_id = threading.get_ident()
        with cls._lock:
            if thread_id in cls._connections:
                connection = cls._connections[thread_id]
                if connection.is_open:
                    connection.close()
                    logger.info(f"Thread {thread_id}: Closing connection.")

    @classmethod
    def close_channel(cls):
        logger.setLevel(Parameters.log_level)
        thread_id = threading.get_ident()
        with cls._lock:
            if thread_id in cls._channels:
                channel = cls._channels[thread_id]
                if channel.is_open:
                    channel.close()
                    logger.info(f"Thread {thread_id}: Closing channel.")

    @classmethod
    def close(cls):
        cls.close_channel()
        cls.close_connection()

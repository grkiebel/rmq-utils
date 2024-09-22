import os
import pika
from .log_config import get_logger


logger = get_logger(__name__)


class Parameters:
    # connection parameters
    host: str = "localhost"
    port: int = 5672
    user: str = "guest"
    pswd: str = "guest"

    # housekeeping parameters
    loop_interval = 2.0
    log_level = "WARNING"

    @classmethod
    def load_from_env(cls):
        cls.host = os.getenv("RMQ_HOST", cls.host)
        cls.port = int(os.getenv("RMQ_PORT", cls.port))
        cls.user = os.getenv("RMQ_USER", cls.user)
        cls.pswd = os.getenv("RMQ_PSWD", cls.pswd)
        cls.loop_interval = float(os.getenv("RMQ_LOOP_INTERVAL", cls.loop_interval))
        cls.log_level = os.getenv("RMQ_LOG_LEVEL", cls.log_level)

    @classmethod
    def set(cls, **kwargs):
        for key, value in kwargs.items():
            if hasattr(cls, key):
                setattr(cls, key, value)

    @classmethod
    def connection(cls):
        return pika.ConnectionParameters(
            host=cls.host,
            port=cls.port,
            credentials=pika.PlainCredentials(cls.user, cls.pswd),
            heartbeat=600,
        )

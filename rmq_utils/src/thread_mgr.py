import queue
import threading
import concurrent.futures
from rmq_utils.src.params import Parameters
from rmq_utils.src.log_config import get_logger


logger = get_logger(__name__)


class Runner:
    def __init__(self):
        self.objs = []

    def __iadd__(self, obj: object):
        """
        Implements the += operator to add an object to the list of managed objects.
        """
        self.objs.append(obj)
        return self

    def start_all(self) -> None:
        """
        Call start() for each object in the list if it has a start() method.
        """
        for obj in self.objs:
            if hasattr(obj, "start") and callable(getattr(obj, "start")):
                obj.start()

    def stop_all(self, timeout: float = None):
        """
        Call stop() for each object in the list if it has a stop() method.
        """
        threads = []
        for obj in self.objs:
            if hasattr(obj, "stop") and callable(getattr(obj, "stop")):
                threads.append(obj.stop())
        self._join_threads(timeout)

    def _join_threads(self, timeout: float = None):
        """
        Wait for all threads in the list to finish.
        """
        threads = [
            obj.get_thread()
            for obj in self.objs
            if hasattr(obj, "get_thread") and callable(getattr(obj, "get_thread"))
        ]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(thread.join, timeout) for thread in threads if thread
            ]
            concurrent.futures.wait(futures)


class ThreadManager:
    """
    Base class for subclasses that run a loop on an internally managed thread.
    A local python queue is used to exchange messages with the client
    so that the internal thread is not blocked by the client.
    """

    def __init__(self, name: str):
        self.name = name
        self.stop_flag = threading.Event()
        self.started = threading.Event()
        self.my_thread = None
        self._local_queue = queue.Queue()
        logger.setLevel(Parameters.log_level)

    def _main_loop(self):
        # override this method in the subclasses
        raise NotImplementedError

    def start(self) -> threading.Thread:
        """
        Start the main loop in its own thread.
        """
        if not self.my_thread or not self.my_thread.is_alive():
            self.my_thread = threading.Thread(target=self._main_loop)
            self.my_thread.start()
            logger.info(f"{self.name}: Starting thread {self.my_thread.ident}")
            self.started.wait(2)
        return self.my_thread

    def stop(self) -> threading.Thread:
        """
        Stop the main loop.
        """
        self.stop_flag.set()
        return self.my_thread

    def get_queue(self) -> queue.Queue:
        return self._local_queue

    def get_thread(self) -> threading.Thread:
        return self.my_thread

    def _get_msg_from_local_queue(self) -> object | None:
        try:
            obj = self._local_queue.get(timeout=Parameters.loop_interval)
            self._local_queue.task_done()
        except queue.Empty:
            obj = None
        return obj


class Consumer(ThreadManager):
    """
    A class pulls messages from the given python queue and sends messages to a callback function.
    """

    def __init__(self, name: str, queue: queue.Queue, callback: callable):
        super().__init__(name)
        self.callback = callback
        self._local_queue = queue

    def _main_loop(self):
        """
        Pulls messages from the local queue and sends them to the callback function.
        """
        logger.info(f"Consumer {self.name}: is running")
        self.started.set()
        while True:
            if self.stop_flag.is_set():
                break
            if not (msg := self._get_msg_from_local_queue()):
                continue
            self.callback(msg)

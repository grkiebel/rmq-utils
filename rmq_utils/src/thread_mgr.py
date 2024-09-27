import queue
import threading
import concurrent.futures
from typing import List
from rmq_utils.src.params import Parameters
from rmq_utils.src.log_config import get_logger


logger = get_logger(__name__)


class Runner:
    """
    A class that manages a list of objects that have start(), stop(), and get_thread() methods.
    """

    def __init__(self, objects: List[object] = None):
        self.objects: List[object] = objects or []

    def __iadd__(self, obj: object):
        """
        Implements the += operator to add an object to the list of managed objects.
        """
        self.objects.append(obj)
        return self

    def start_all(self) -> None:
        """
        Call start() for each object in the list if it has a start() method.
        """
        for obj in self.objects:
            if hasattr(obj, "start") and callable(getattr(obj, "start")):
                obj.start()

    def stop_all(self, timeout: float = None, wait: bool = True):
        """
        Call stop() for each object in the list if it has a stop() method.
        """
        threads = []
        for obj in self.objects:
            if hasattr(obj, "stop") and callable(getattr(obj, "stop")):
                threads.append(obj.stop())
        if wait:
            self.join_all(timeout)

    def join_all(self, timeout: float = None):
        """
        Wait for all threads in the list to finish.
        """
        threads = [
            obj.get_thread()
            for obj in self.objects
            if hasattr(obj, "get_thread") and callable(getattr(obj, "get_thread"))
        ]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(thread.join, timeout) for thread in threads if thread
            ]
            concurrent.futures.wait(futures)


class ThreadManager:
    """
    Base class that provides a framework that runs a function on an internal thread.
    The function itself is implemented in a subclass. A local queue is maintained
    for buffering communication with client code to prevent it from blocking the internal thread.
    This base class is not meant to be directly instantiated.
    if attempted.
    """

    def __init__(self, name: str):
        self.name = name
        self.stop_flag = threading.Event()
        self.started = threading.Event()
        self.my_thread = None
        self._local_queue = queue.Queue()
        logger.setLevel(Parameters.log_level)

    def start(self) -> threading.Thread:
        """
        Start the main loop on the internal thread.
        """
        if not self.my_thread or not self.my_thread.is_alive():
            self.my_thread = threading.Thread(target=self._main_loop)
            self.my_thread.start()
            logger.info(f"{self.name}: Starting thread {self.my_thread.ident}")
            self.started.wait(2)
        return self.my_thread

    def stop(self) -> threading.Thread:
        """Signal the main loop to stop and return a reference to the thread on which it is running."""
        self.stop_flag.set()
        return self.my_thread

    def get_thread(self) -> threading.Thread:
        """Return a reference to the thread on which the main loop is running."""
        return self.my_thread

    def get_queue(self) -> queue.Queue:
        """Return a reference to the local queue."""
        return self._local_queue

    def get_msg_from_local_queue(self) -> object | None:
        """Get a message from the local queue if there is one available. Otherwise, return None after a timeout."""
        try:
            obj = self._local_queue.get(timeout=Parameters.loop_interval)
            self._local_queue.task_done()
        except queue.Empty:
            obj = None
        return obj

    def _main_loop(self):
        # override this method in the subclasses
        raise NotImplementedError


class Consumer(ThreadManager):
    """
    This class pulls messages from the given python queue and sends them to a callback function.
    It inherits from ThreadManager and overrides the _main_loop method.
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
            if not (msg := self.get_msg_from_local_queue()):
                continue
            self.callback(msg)

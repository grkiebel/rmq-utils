import os
import random
import string
import threading

# import concurrent.futures

random.seed(os.getpid())


# def start_all(objs: list[object]) -> None:
#     """
#     Call start() for each object in the list if it has a start() method.
#     """
#     for obj in objs:
#         if hasattr(obj, "start") and callable(getattr(obj, "start")):
#             obj.start()


# def join_threads(threads: list[threading.Thread], timeout: float = None):
#     """
#     Wait for all threads in the list to finish.
#     """
#     with concurrent.futures.ThreadPoolExecutor() as executor:
#         futures = [executor.submit(thread.join, timeout) for thread in threads]
#         concurrent.futures.wait(futures)


# def stop_all(objs: list[object], timeout: float = None):
#     """
#     Call stop() for each object in the list if it has a stop() method.
#     """
#     threads = []
#     for obj in objs:
#         if hasattr(obj, "stop") and callable(getattr(obj, "stop")):
#             threads.append(obj.stop())
#     join_threads(threads, timeout)


def make_id(format_string: str) -> str:
    """
    The function will generate a string according to the given format string.
    The format string will contain specifiers that expand to multiple letters or digits.
    These specifiers are designated by a three character group following the pattern "<x:n>"
    where n is the number of characters to generate and x is one of the following Letters:
    "A" will expand to n uppercase letters;
    "a" will expand to n lowercase letters;
    "N" will expand to an n digit number.
    All other characters in the format string will pass to the output string unchanged.
    """
    start = "<"
    end = ">"
    separator = ":"
    result = []
    i = 0
    while i < len(format_string):

        if (
            format_string[i] == start
            and i + 4 < len(format_string)
            and format_string[i + 4] == end
            and format_string[i + 2] == separator
        ):
            specifier = format_string[i + 1]
            length = int(format_string[i + 3])
            if specifier == "A":
                result.append("".join(random.choices(string.ascii_uppercase, k=length)))
            elif specifier == "a":
                result.append("".join(random.choices(string.ascii_lowercase, k=length)))
            elif specifier == "N":
                result.append("".join(random.choices(string.digits, k=length)))
            i += 5
        else:
            result.append(format_string[i])
            i += 1
    return "".join(result)


if __name__ == "__main__":

    for i in range(5):
        print(make_id(f"TX-<A:2>-<A:1><N:4>-{i}"))

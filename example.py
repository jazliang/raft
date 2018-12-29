import threading
import time


def func():
    threading.Timer(2, func).start()
    print("dadada")

func()

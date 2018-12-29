import asyncio
import threading
import logging


class RaftTimer:
    def __init__(self, interval, callback):
        self.interval = interval
        self.callback = callback
        self.is_active = False


    def start(self):
        self.is_active = True
        self.thread_handler = threading.Timer(self.interval, self._run)
        self.thread_handler.start()
        # logging.debug("Timer started.")

    def _run(self):
        if self.is_active:
            import logging
            logging.basicConfig(level=logging.DEBUG)
            # logging.debug("Timer: run callback")
            self.callback()
            self.thread_handler = threading.Timer(self.interval, self._run)

    def stop(self):
        self.is_active = False
        self.thread_handler.cancel()

    def reset(self):
        self.stop()
        self.start()

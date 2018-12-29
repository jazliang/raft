from .board import Board
import queue


class MemoryBoard(Board):
    """
    A board residing in memory
    """

    def __init__(self):
        Board.__init__(self)
        self.board = queue.Queue()

    def post_message(self, message):
        self.board.put(message)

        # self.board = sorted(self.board,
        #                      key=lambda a: a.timestamp, reverse=True)

    def get_message(self):
        # get() operation is blocking
        # i.e. block if queue is empty until an item is available
        return self.board.get(block=False)

    def reset(self):
        self.board = queue.Queue()

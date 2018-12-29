
class Board(object):
    """
    Boards act like queues, and allow multiple clients to write to them.
    """
    def __init__(self):
        self._owner = None

    def set_owner(self, owner):
        self._owner = owner

    def post_message(self, message):
        """
        This will post a message to the board.
        """

    def get_message(self):
        """
        This will get the next message from the board.
        """
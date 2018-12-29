import time


class BaseMessage(object):
    APPEND_ENTRIES = 0
    REQUEST_VOTE = 1
    REQUEST_VOTE_RESPONSE = 2
    RESPONSE = 3

    names = {
        0: 'APPEND_ENTRIES',
        1: 'REQUEST_VOTE',
        2: 'REQUEST_VOTE_RESPONSE',
        3: 'RESPONSE'
    }

    def __init__(self, sender, receiver, term, data):
        self.timestamp = int(time.time())

        self.sender = sender
        self.receiver = receiver
        self.data = data
        self.term = term
        self.type = None

    def __str__(self):
        return 'sender %d, receiver %d, term %d, type %s' % (self.sender,
                                                             self.receiver,
                                                             self.term,
                                                             self.names[self.type])




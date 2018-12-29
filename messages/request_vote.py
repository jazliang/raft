from .base import BaseMessage


class RequestVoteMessage(BaseMessage):
    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)
        self.type = BaseMessage.REQUEST_VOTE


class RequestVoteResponseMessage(BaseMessage):
    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)
        self.type = BaseMessage.REQUEST_VOTE_RESPONSE

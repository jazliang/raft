from .base import BaseMessage


class ResponseMessage(BaseMessage):
    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)
        self.type = BaseMessage.RESPONSE

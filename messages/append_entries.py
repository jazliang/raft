from messages.base import BaseMessage


class AppendEntriesMessage(BaseMessage):
    def __init__(self, sender, receiver, term, data):
        BaseMessage.__init__(self, sender, receiver, term, data)
        self.type = BaseMessage.APPEND_ENTRIES

import time
import random
import logging
# logging.basicConfig(level=logging.DEBUG)
from timer import RaftTimer
import threading
import time

from messages.base import BaseMessage
from messages.response import ResponseMessage
from messages.request_vote import RequestVoteResponseMessage
from messages.request_vote import RequestVoteMessage
from messages.append_entries import AppendEntriesMessage


def get_ran_ele_timeout():
    # return random.randint(7000, 10000) / 1000
    return random.randint(1500, 2500) / 1000


def get_req_vote_interval():
    # return random.randint(1000, 4000) / 1000
    return random.randint(1500, 2500) / 1000


class State(object):
    def __init__(self, server):
        self.server = server

    def set_server(self, server):
        self.server = server

    def on_message(self, message):
        """
        This method is called when a message is received,
        and calls one of the other corresponding methods
        that this state reacts to.

        :param message:
        :return:
        """

        # Type of the received message
        _type = message.type

        # Receives a higher term, if this serve is a candidate or leader, convert to follower
        if message.term > self.server.current_term:
            self.server.current_term = message.term

            # TODO: File
            # with open('/tmp/%d-current_term.txt' % self.server.name, 'w') as f:
            #     f.write(str(self.server.current_term))

            self.server.state.to_follower()
            self.server.voted_for = None

            # TODO: File
            # with open('/tmp/%d-voted_for.txt' % self.server.name, 'w') as f:
            #     f.write(str(self.server.voted_for))

        # Is the messages.term < ours? If so we need to tell
        #   them this so they don't get left behind.
        elif message.term < self.server.current_term:
            self._send_response_message(message, yes=False)
            return self, None

        # Receive append_entries from leader
        if _type == BaseMessage.APPEND_ENTRIES:
            return self.on_append_entries(message)

        # Receive request_vote from candidate
        elif _type == BaseMessage.REQUEST_VOTE:
            return self.on_vote_request(message)

        # Request_vote_response sent to candidate
        elif _type == BaseMessage.REQUEST_VOTE_RESPONSE:
            return self.on_vote_received(message)

        elif _type == BaseMessage.RESPONSE:
            return self.on_response_received(message)


    def on_leader_timeout(self, message):
        """This is called when the leader timeout is reached."""

    def on_vote_request(self, message):
        """This is called when there is a vote request."""

    def on_vote_received(self, message):
        """This is called when this node recieves a vote."""

    def on_append_entries(self, message):
        """This is called when there is a request to
        append an entry to the log.

        """

    def on_response_received(self, message):
        """This is called when a response is sent back to the Leader"""

    def on_client_command(self, message):
        """This is called when there is a client request."""

    def _send_response_message(self, msg, yes=True):
        response = ResponseMessage(sender=self.server.name,
                                   receiver=msg.sender,
                                   term=self.server.current_term,
                                   data={"response": yes,
                                         "current_term": self.server.current_term,})
        self.server.send_message_response(response)


class Voter(State):

    def __init__(self, server):
        State.__init__(self, server)

    def on_vote_request(self, message):
        if message.term < self.server.current_term:
            self.send_vote_response_message(message, yes=False)
            logging.warning('Voted NO to #%d' % message.sender)
            return

        if message.term == self.server.current_term and self.server.voted_for is not None:
            self.send_vote_response_message(message, yes=False)
            logging.warning('Voted NO to #%d' % message.sender)
            return

        # Votes YES
        self.server.voted_for = message.sender

        # TODO: File
        # with open('/tmp/%d-voted_for.txt' % self.server.name, 'w') as f:
        #     f.write(str(self.server.voted_for))

        self.server.current_term = message.term

        # TODO: File
        # with open('/tmp/%d-current_term.txt' % self.server.name, 'w') as f:
        #     f.write(str(self.server.current_term))

        self.send_vote_response_message(message, yes=True)
        logging.warning('Voted YES to #%d' % message.sender)


    def send_vote_response_message(self, msg, yes=True):
        vote_response = RequestVoteResponseMessage(sender=self.server.name,
                                                   receiver=msg.sender,
                                                   term=msg.term,
                                                   data={"response": yes})
        self.server.send_message_response(vote_response)


class Follower(Voter):
    def __init__(self, server):
        Voter.__init__(self, server)
        # self._timeout = timeout
        # self._timeout_time = self._next_timeout()
        self.election_timer = RaftTimer(get_ran_ele_timeout(), self.to_candidate)
        self.election_timer.start()
        logging.warning("Server %d Follower Term %d" % (self.server.name, self.server.current_term))


    def on_append_entries(self, message):
        # Reset election timeout
        # TODO: reset timeout in any case?
        self.election_timer.reset()

        if message.term < self.server.current_term:
            # TODO: after network partition
            self._send_response_message(message, yes=False)
            return

        # Heartbeat message
        if message.data is None:
            self._send_response_message(message, yes=True)
            return
        else:
            raise NotImplementedError

    def to_candidate(self):
        logging.warning("Server #%d: Follower -> Candidate" % self.server.name)
        self.close()
        self.server.state = Candidate(self.server)
        self.server.message_inbox.reset()

    def to_follower(self):
        logging.warning("Server #%d: Follower -> Follower" % self.server.name)
        self.close()
        self.server.state = Follower(self.server)
        self.server.message_inbox.reset()

    def close(self):
        self.election_timer.stop()


class Candidate(Voter):
    def __init__(self, server):
        Voter.__init__(self, server)

        self.server.current_term += 1

        # TODO: File
        # with open('/tmp/%d-current_term.txt' % self.server.name, 'w') as f:
        #     f.write(str(self.server.current_term))

        # Always votes for itself
        self.server.voted_for = self.server.name

        # TODO: File
        # with open('/tmp/%d-voted_for.txt' % self.server.name, 'w') as f:
        #     f.write(str(self.server.voted_for))

        self.election_timer = RaftTimer(get_ran_ele_timeout(), self.to_candidate)
        self.election_timer.start()

        self.votes = {self.server.name: True}
        self.has_not_vote = set([node[0] for node in self.server.nodes if node[0] != self.server.name])
        self.yes_votes_count = 1
        logging.warning("Server %d Candidate Term %d" % (self.server.name, self.server.current_term))
        self.request_vote_message = RequestVoteMessage(sender=self.server.name,
                                                       receiver=None,
                                                       term=self.server.current_term,
                                                       data=None)
        self.lock = threading.Lock()
        self._start_election()

        self.request_vote_handler = None


    def _start_election(self):
        # All votes have been received, no need to request
        with self.lock:
            if len(self.has_not_vote) == 0:
                return

            # if len(self.has_not_vote) == 0 or not isinstance(self.server.state, Candidate):
            #     return

            # "Queue" a call to itself that will be run in the future
            self.server.send_message_to_all(self.request_vote_message, list(self.has_not_vote))
            self.request_vote_handler = threading.Timer(get_req_vote_interval(), self._start_election).start()
            # logging.warning('Vote requests sent.')


    def to_follower(self):
        logging.warning("Server #%d: Candidate -> Follower" % self.server.name)
        self.close()
        self.server.state = Follower(self.server)
        self.server.message_inbox.reset()

    def to_candidate(self):
        logging.warning("Server #%d: Candidate -> Candidate" % self.server.name)
        self.close()
        self.server.state = Candidate(self.server)
        self.server.message_inbox.reset()

    def to_leader(self):
        logging.warning("Server #%d: Candidate -> Leader" % self.server.name)
        self.close()
        self.server.state = Leader(self.server)
        self.server.message_inbox.reset()


    def on_vote_received(self, message):
        with self.lock:
            if message.sender in self.votes:
                logging.warning("Receiving vote from server #%d, but it has already voted." % message.sender)
                return
            # assert message.sender not in self.votes, "Server #%d already voted." % message.sender

            logging.warning("Receiving vote from server #%d" % message.sender)
            vote_response = message.data['response']
            self.votes[message.sender] = vote_response
            self.has_not_vote.remove(message.sender)

            if vote_response:  # Voted YES
                self.yes_votes_count += 1

            logging.warning("Server %d YES Votes: %d" % (self.server.name, self.yes_votes_count))

            if self.yes_votes_count >= self.server.n_nodes // 2 + 1:
                self.to_leader()


    def close(self):
        self.election_timer.stop()
        if self.request_vote_handler is not None:
            self.request_vote_handler.cancel()


class Leader(State):

    def __init__(self, server):
        State.__init__(self, server)

        logging.warning("Server %d Leader Term %d" % (self.server.name, self.server.current_term))

        self.heartbeat_interval = 0.7
        self.heartbeat_handler = None
        self.heartbeat_list = [node[0] for node in self.server.nodes if node[0] != self.server.name]
        self._send_heartbeat()

    def to_follower(self):
        logging.debug("Server #%d: Leader -> Follower" % self.server.name)
        self.close()
        self.server.state = Follower(self.server)
        self.server.message_inbox.reset()

    def on_response_received(self, message):
        # assert message.data is None

        if message.term > self.server.current_term:
            # A larger leader is found. Thus this leader steps down.
            # This scenario will happen after the cluster recovers from a network partition.
            self.server.current_term = message.term

            # TODO: File
            # with open('/tmp/%d-current_term.txt' % self.server.name, 'w') as f:
            #     f.write(str(self.server.current_term))

            self.to_follower()


    def _send_heartbeat(self):
        # "Queue" a call to itself that will be run in the future
        self.heartbeat_handler = threading.Timer(self.heartbeat_interval, self._send_heartbeat).start()

        message = AppendEntriesMessage(sender=self.server.name,
                                       receiver=None,
                                       term=self.server.current_term,
                                       data=None)
        self.server.send_message_to_all(message, self.heartbeat_list)

    def close(self):
        if self.heartbeat_handler is not None:
            self.heartbeat_handler.cancel()

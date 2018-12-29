import rpyc
import sys
import copy
from threading import Lock, Thread
import os
from states import *

from boards.memory_board import MemoryBoard

import logging
logging.basicConfig(level=logging.WARNING)


class RaftNode(rpyc.Service):
    """
    A RAFT RPC server class.
    """

    def __init__(self, config, server_id):
        """
        Initialize the class using the config file provided and also initialize
        any data structures you may need.

        :param config:
        """
        with open(config, 'r') as f:
            lines = f.readlines()
            self.n_nodes = int(lines[0].split()[1])

            self.nodes = []
            self.nodes_conns = [None] * self.n_nodes  # RPC Connections

            for _id in range(self.n_nodes):
                _host, _port = lines[_id + 1].split(' ')[1].split(':')
                _port = int(_port)
                self.nodes.append((_id, _host, _port))

        self.lock = Lock()
        self.name = server_id

        # TODO: File
        use_persistent = False

        if use_persistent:
            current_term_filename = '/tmp/%d-current_term.txt' % self.name
            if os.path.isfile(current_term_filename):
                with open(current_term_filename, 'r') as f:
                    self.current_term = eval(f.read())
            else:
                self.current_term = 0
                with open(current_term_filename, 'w') as f:
                    f.write(str(self.current_term))

            voted_for_filename = '/tmp/%d-voted_for.txt' % self.name
            if os.path.isfile(voted_for_filename):
                with open(voted_for_filename, 'r') as f:
                    self.voted_for = eval(f.read())
            else:
                # This is used to ensure a voter only votes for one node in a term
                self.voted_for = None
                with open('/tmp/%d-voted_for.txt' % self.name, 'w') as f:
                    f.write(str(self.voted_for))
        else:
            self.voted_for = None
            self.current_term = 0

        self.state = Follower(self)
        self.state.set_server(self)

        # A queue of event messages
        self.message_inbox = MemoryBoard()
        self.message_inbox.set_owner(self)

        self.consume_inbox_thread = Thread(target=self.consume_inbox)
        self.consume_inbox_thread.daemon = True
        self.consume_inbox_thread.start()


    def send_message_to_all(self, message, receivers=None):
        if receivers is None:
            receivers = [node[0] for node in self.nodes]

        def send_one_message(_message, _node_name, _host, _port):
            _message.receiver = _node_name  # The id of the receiver

            try:
                if self.nodes_conns[_message.receiver] is None:
                    self.nodes_conns[_message.receiver] = rpyc.connect(_host, _port, config={'allow_pickle': True})

                # async_post_message = rpyc.async_(conn.root.post_message)
                # async_post_message(_message)
                self.nodes_conns[_message.receiver].root.post_message(_message)

            except ConnectionRefusedError:
                self.nodes_conns[_message.receiver] = None
            except EOFError:
                self.nodes_conns[_message.receiver] = None
                # logging.warning('Connection to server #%d refused.' % _node_name)


        for node_name in receivers:
            _, _host, _port = self.nodes[node_name]
            Thread(target=send_one_message, args=(copy.deepcopy(message), node_name, _host, _port)).start()
            # Thread(target=send_one_message, args=(message, node_name, _host, _port)).start()

            # try:
            #     conn = rpyc.connect(_host, _port, config={'allow_pickle': True})
            #     message.receiver = node_name  # The id of the receiver
            #     async_post_message = rpyc.async_(conn.root.post_message)
            #     async_post_message(message)
            #     # conn.root.post_message(message)
            #     # conn.close()
            # except ConnectionRefusedError:
            #     logging.debug('Connection to server #%d refused.' % node_name)


    def send_message_response(self, message):
        # logging.warning('Server #%d sending Message: %s' % (self.name, message))

        def send_one_message(_message, _node_name, _host, _port):
            try:
                if self.nodes_conns[_message.receiver] is None:
                    self.nodes_conns[_message.receiver] = rpyc.connect(_host, _port, config={'allow_pickle': True})

                _message.receiver = _node_name  # The id of the receiver
                # async_post_message = rpyc.async_(conn.root.post_message)
                # async_post_message(_message)
                self.nodes_conns[message.receiver].root.post_message(_message)
                logging.warning('Responded to #%d (host: %s, port: %d)' % (_message.receiver, _host, _port))
            except ConnectionRefusedError:
                self.nodes_conns[message.receiver] = None
            except EOFError:
                self.nodes_conns[message.receiver] = None
                # logging.warning('Connection to server #%d refused. [In send_message_response]' % _node_name)

        _node_name, _host, _port = self.nodes[message.receiver]
        Thread(target=send_one_message, args=(message, _node_name, _host, _port)).start()

        # try:
        #     _, _host, _port = self.nodes[message.receiver]
        #     conn = rpyc.connect(_host, _port, config={'allow_pickle': True})
        #     async_post_message = rpyc.async_(conn.root.post_message)
        #     async_post_message(message)
        #     # conn.root.post_message(message)
        #     # conn.close()
        # except ConnectionRefusedError:
        #     logging.debug('Connection to server #%d refused.' % message.receiver)


    def consume_inbox(self):
        from queue import Empty

        while True:
            try:
                # get_message() operation is blocking
                # i.e. block if inbox is empty until an item is available
                message = self.message_inbox.get_message()
                logging.warning('Consuming: %s' % message)
                self.state.on_message(message)
            except Empty:
                pass


    def exposed_post_message(self, message):
        """
        Allows clients (other servers) to sent message to this server.

        :param message:
        :return:
        """
        message = copy.deepcopy(message)
        with self.lock:
            self.message_inbox.post_message(message)


    def exposed_is_leader(self):
        """
        x = is_leader(): returns True or False, depending on whether
        this node is a leader

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call

        :return:
        """
        with self.lock:
            return isinstance(self.state, Leader)


if __name__ == '__main__':
    from rpyc.utils.server import ThreadPoolServer, ThreadedServer

    config_file = sys.argv[1]
    server_number = int(sys.argv[2])
    port = int(sys.argv[3])

    server = ThreadedServer(RaftNode(config_file, server_number), port=port)
    # server = ThreadPoolServer(RaftNode(config_file, server_number), port=port)
    server.start()


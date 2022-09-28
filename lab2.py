"""
This is lab2.py, a bully algorithm code project. Nodes enact elections, declares a winner (of highest value)
and then ends election. Every time a node joins, it enacts an election. Highest value node (via PID) wins.
The rest lose the election and go idle. Every time a node joins, an election occurs which propogates to higher
value nodes on the server. The Node is given the list of other nodes from the GCD2.

Parameters for the program are: "python3 lab2.py GCDPORT Days-Until-Birthday SUID"
Param: Days-Until-Birthday is a number, like 80.
Param: GCDPORT is the port number GCD2.py is running on.
Param: SUID is a person's SUID number.

__author__ = greg reneris
__version__ = 1.0

"""

import logging as log
log.basicConfig(level=log.INFO, format = '%(asctime)s %(message)s', datefmt='%I:%M:%S %p:')

import socket
import sys
import pickle
import datetime
import time
import selectors
from enum import Enum
from pprint import pprint

TIMEOUT_TIME = 1.5 # 1.5 seconds for socket to send and recieve
BUFFER_SIZE = 1024 # number of bytes in message
CHECK_INTERVAL = 0.1


class State(Enum):
    """
    Enumeration of states a peer can be in for the Lab2 class.
    """
    QUIESCENT = 'QUIESCENT'  # Erase any memory of this peer

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'
    SEND_VICTORY = 'COORDINATOR'
    SEND_OK = 'OK'

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING'  # When I've done an accept on their connect to my server


class Lab2(object):
    """
    Lab2 contains all needed init information and relevant functions to run a node election
    Highest value node wins the election and all other nodes lose and go idle.
    When a new node joins the group, a new election begins.
    Nodes get the group list from the running GCD.
    A victorious node declares itself the winner, and tells the other nodes they lost.

    """

    def __init__(self, gcd_address, next_birthday, su_id):
        """Constructs a Lab2 object to talk to the Group Coordinate Daemon"""
        self.gcd_address = (gcd_address[0], int (gcd_address[1]))
        days_to_birthday = int(next_birthday)
        self.pid = (days_to_birthday, int(su_id))
        self.members = {}
        self.state = State.SEND_ELECTION
        self.bully = None #None means an election is pending, otherwise this will be the PID of the leader.
        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = self.start_a_server()
        self.waiting = {}

   
    def start_a_server(self):
        """
        Calls socket to create a socket.
        binds to the host and assigns port.
        listens, and then registers the selector to run.

        returns a server socket with the socket address.
        """
        server_socket = socket.socket()
        server_socket.bind(('localhost', 0))
        server_socket.listen(3)
        server_socket.setblocking(False)
        self.selector.register(server_socket, selectors.EVENT_READ, (self.server_accept, None))
        log.info(f'server started on {server_socket.getsockname()}')
        return ( server_socket , server_socket.getsockname())


    def server_accept(self, sock, _):
        """
        This server_accept accepts a connectin through sock and registers,
        waiting for a read confirmation.
        """
        conn, addr = sock.accept()
        log.info(f'accepted connection from {addr}')
        conn.setblocking(False)
        self.selector.register(conn, selectors.EVENT_READ, (self.server_read, None))

        
    def server_read(self, sock, _):
        """
        This function handles the responses from other nodes
        and calls appropriate functionality.
        """
        buffer = sock.recv(BUFFER_SIZE)
        if len(buffer) == 0:
            sock.close()
            self.selector.unregister(sock)
            return

        assert len(buffer)
        cmd, data = pickle.loads( buffer )
        log.info(f"server recieves: {cmd} ")
        
        if cmd == 'COORDINATOR':           
            self.server_recv_coordinator(sock, data)
           
        elif cmd == 'ELECTION':
            self.server_recv_election(sock, data)
        
        else:
            log.error(f'Unknown message: {cmd}')

    
    def server_recv_election(self, sock, memberlist):    
        """
        This function updates the memberlist, starts 
        an election and responds with okay to lower members.
        """
        self.members.update(memberlist) #this updates members with memberlist.
        self.set_state(State.SEND_ELECTION)
        self.start_election('received a vote')
        sock.send(pickle.dumps( ("OKAY", None) ))
        sock.close()
        self.selector.unregister(sock)
        


    def server_recv_coordinator(self, sock, memberlist):
        """
        function called on receiving coordination message.
        Then calls to set state to quiescent. 
        """
        self.members.update(memberlist) #another way to upadte members with memberlist.
        self.set_state(State.QUIESCENT)
        log.info(f'lost election - going idle')
      

    def send_coordinator_to_all(self):
        """
        This function connects to all other nodes
        and sends them the coordinator messaage.
        """
        for key, address in self.members.items():
            if key != self.pid:
                sock = socket.socket() #calling socket returns a socket.
                sock.setblocking(False)
                sock.connect_ex(address) #connect_ex waits
                self.selector.register(sock, selectors.EVENT_WRITE, (self.send_coordinator, key))


    def send_coordinator(self, sock, key):
        """
        Sends the coordinator message to other nodes via pickle
        and logs an error if a connection fails.
        """
        message = ('COORDINATOR', (self.members) )
        log.info(f"send COORDINATOR to {key}")
        try:
            sock.send( pickle.dumps(message) )
        except socket.error as err:
            log.info(f'coordinator message failed to connect to a node')
        sock.close()
        self.selector.unregister(sock)


    def set_state(self, new_state):
        """ sets the state of the node to the param of new_state"""

        log.info(f'updating state {self.state} -> {new_state}')
        self.state = new_state


    def run_forever(self):
        """ 
        runs the node forever and checks at regular intervals for more
        events. Then calls callback on the fileobj.
        """
        self.start_election('at startup')
        while True:
            events = self.selector.select(CHECK_INTERVAL)
            for key, mask in events:
                callback, address = key.data
                #print(key.fileobj.fileno(), callback)
                callback(key.fileobj, address)
            self.check_timeouts()


    def check_timeouts(self):
        """
        if the state is waiting_for_a_message,
        will empty the to_delete list.
        will check to see if there are any members left in the 
        waiting member list.
        Will declare victory on self if there are none waiting.
        members are deleted when they time out.
        """

        if self.state == State.WAITING_FOR_ANY_MESSAGE:
            to_delete = []
            for key, value in self.waiting.items():
                sock, end_time = value
                if time.time() > end_time: #this node timed out
                    log.info(f"timeout: {key}")
                    to_delete.append(key)
                    try:                   
                        self.selector.unregister(sock)
                        sock.close()
                    except (ValueError, socket.error) as err:
                        pass

            for key in to_delete:
                del self.waiting[key]

            if len(self.waiting) == 0:
                self.set_winner(self.pid)
        


    def start_election(self, reason):
        """starts the election"""
        log.info(f"starting election ({reason}). I am: {self.pid}")
        gcd_socket = socket.socket() #calling socket returns a socket.
        gcd_socket.setblocking(False)
        gcd_socket.connect_ex(self.gcd_address)
        self.selector.register(gcd_socket, selectors.EVENT_WRITE, (self.send_gcd_join, None))


    def send_gcd_join(self, sock, _):
        """
        joins the GCD server
        adds the read_gcd_members sockets to the selector.register.
        when socket is ready to ready, the main loop calls read_gcd_members.
        """
        message = ('JOIN',  (self.pid, self.listener_address ) )
        sock.send(pickle.dumps(message))
        self.selector.modify(sock, selectors.EVENT_READ, (self.read_gcd_members, None))


    def read_gcd_members(self, sock, _):
        """
        this method writes / updates the node's member list.
        """
        response = pickle.loads(sock.recv(BUFFER_SIZE))
        #print(response)

        sock.close()
        self.selector.unregister(sock)
     
        self.waiting = {}
        self.members = response
        for key, address in self.members.items():
            log.info(f'Member: {key}, {address}')
            if (key > self.pid):
                sock = socket.socket()
                sock.setblocking(False)
                sock.connect_ex(address)
                self.selector.register(sock, selectors.EVENT_WRITE, (self.send_election, key))      
                self.waiting[key] = (sock, time.time()+TIMEOUT_TIME)

        self.set_state(State.WAITING_FOR_ANY_MESSAGE)
        if len(self.waiting) == 0:
            self.set_winner(self.pid)

    
    def send_election(self, sock, key):
        """
        sends the ELECTION message to other nodes.
        will try except for errors.
        closes socket and unregisters at the end.
        """
        message = ('ELECTION', (self.members) )
        log.info(f"Sending election to {key}")

        try:
            sock.send( pickle.dumps(message) )
        except socket.error as err:
            log.info("message failed to send" )

        self.selector.modify(sock, selectors.EVENT_READ, (self.read_election_response, key))



    def read_election_response(self, sock, key):
        """
        read_election_response is a function that takes in a pickled
        packet of "OKAY" and "NONE" as a response from the election.
        This prevents multiple nodes from declaring victory.
        If the message is accepted, the current node state goes to 
        idle, which is QUIESCENT.
        Socket is closed and unregistered.
        """
        message = sock.recv(BUFFER_SIZE)
        if len(message):
            cmd, data = pickle.loads(message)
            log.info(f'election response: {cmd} from {key}')
            if cmd == 'OKAY':
                del self.waiting[key]
                self.set_state(State.QUIESCENT)
        sock.close()
        self.selector.unregister(sock)


    def set_winner(self, winner):
        """
        sets the self.bully as the winner, the current node.
        if the bully is the same as current node (extra verification)
        sends out coordinator message declaring victory.
        """
        self.bully = winner
        log.info(f"current winner is: {winner}")
        if(self.bully == self.pid):
            self.send_coordinator_to_all()
            log.info("******** WINNING ELECTION ********")
            self.set_state(State.SEND_VICTORY)
            self.waiting = {} #if node is winner, clear the waiting list.
        



if __name__ == '__main__':
    log.info('starting bully program')

    if len(sys.argv) != 4:
        print("Usage: python lab2.py GCDPORT DAYS-UNTIL-BDAY(AS NUM) SUID")
        exit(1)       #      0          1       2                   3
   
    gcd_port, days_to_bday, suid = sys.argv[1:]
    lab2 = Lab2( ('localhost', gcd_port), days_to_bday, suid )

    lab2.run_forever()
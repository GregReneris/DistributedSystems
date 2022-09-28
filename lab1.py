"""
This is the program summary.

This program is designed to connect to a server 
based on user command line arguements, and then
creates a dictionary of host port pairs.

The program reaches out to those dictionary members with a 
hello message. Program prints upon success
and handles common communication errors.

__version__ = '1.0'
__author__ = 'Gregory Reneris'

"""


import socket  #imports socket lib
import sys     #imports system.
import pickle  #imports pickle stream
from pprint import pprint #imports pretty print.


if len(sys.argv) != 3:
       print ('Usage: python lab1.py HOST PORT');
       exit(1);


host = sys.argv[1] #host  to use
port = int(sys.argv[2]) #port  to use

def send_recv(host, port, msg):
    """
    This function definition for send_rect, a function that sends a request
    to the server, waits for a response 
    and returns the unpickled response.
   
    Parameters:
        host: the host to connect to.
        port: the port to connect to.
        msg: the message to send.

    Returns:
         unpickled response.

    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1.5) #1.5 seconds = 1500ms
        s.connect((host, port))
        s.sendall( pickle.dumps(msg) )
        data = s.recv(1024)
        return pickle.loads( data ) #unpickles response and returns it.

try:
    print(f"JOIN ('{host}': {port})" )
    member_list = send_recv(host, port, 'JOIN')
    for mem in member_list:
        h = mem['host'] #member's host
        p = mem['port'] #member's port
        try:
            print(f"HELLO to {mem} ")
            resp = send_recv(h,p,'HELLO')
            print(resp)
        except(socket.timeout, socket.error) as err:
            print(f"Cannot connect: {h}:{p}: ", err)
except(socket.timeout, socket.error) as err:
    print("cannot get member list" ,err)








"""Utils file.

This file is to house code common between the Manager and the Worker

"""
import socket
import json

def TCP_client(host, port, message_dict):
	# create an INET, STREAMing socket, this is TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # connect to the server
        sock.connect((host, port))
        # send a message
        message = json.dumps(message_dict)
        sock.sendall(message.encode('utf-8'))

def UDP_client(host, port, message_dict):
    # Create an INET, DGRAM socket, this is UDP
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        # Connect to the UDP socket on server
        sock.connect((host, port))
        # Send a message
        message = json.dumps(message_dict)
        sock.sendall(message.encode('utf-8'))


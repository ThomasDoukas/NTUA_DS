import config
import socket
import hashlib
import pickle

from flask_cors import CORS
from argparse import ArgumentParser
from flask import Flask, Blueprint, jsonify, request

from Node import Node
from endpoints.chord import chord, node
from endpoints.client import client

# Define the flask environment and register the blueprint with the endpoints.
app = Flask(__name__)
app.register_blueprint(chord)
app.register_blueprint(client)
CORS(app)

# All nodes are aware of the ip and the port of the bootstrap
# node, in order to communicate with it when entering the network.
BOOTSTRAP_IP = config.BOOTSTRAP_IP
BOOTSTRAP_PORT = config.BOOTSTRAP_PORT
BOOTSTRAP_ADDRESS = "{}:{}".format(BOOTSTRAP_IP, BOOTSTRAP_PORT)

# Get the IP address of the device.
if config.LOCAL:
    address = BOOTSTRAP_IP
else:
    hostname = socket.gethostname()
    address = socket.gethostbyname(hostname)

if __name__ == '__main__':
    
    parser = ArgumentParser(description='Enter the cult...')
    
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional_arguments')

    required.add_argument(
        '-p', type=int, help='port to listen on', required=True)
    optional.add_argument('-bs', action='store_true',
                          help='set if the current node is the bootstrap')

    # Parse the given arguments.
    args = parser.parse_args()
    port = args.p
    is_bootstrap = args.bs
    
    b = "{}:{}".format(address, port).encode()
    ID = hashlib.sha1(b).hexdigest()
    print('Adding node: {}'.format(ID))

    # Initialize node, and add method of joining the chord ring.
    node.IP = address
    node.port = port
    node.ID = ID
    
    if is_bootstrap:
        node.create()
        with open('input/insert.txt') as f:
            lines = f.readlines()
        for line in lines:
            k, v = line.split(', ')
            v = v.split('\n')[0]
            enc = k.encode()
            hk = hashlib.sha1(enc).hexdigest()
            node.storage[hk] = v
    else:
        node.join(BOOTSTRAP_ADDRESS)
        
    # Listen in the specified address (address:port)
    app.run(host=address, port=port)
    

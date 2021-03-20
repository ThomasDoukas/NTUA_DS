from config import *
import socket
import hashlib
import pickle

from flask_cors import CORS
from time import time
from argparse import ArgumentParser
from flask import Flask, Blueprint, jsonify, request

from Node import Node
from endpoints.chord import chord, node
from endpoints.client import client

app = Flask(__name__)
app.register_blueprint(chord)
app.register_blueprint(client)
CORS(app)

BOOTSTRAP_ADDRESS = "{}:{}".format(BOOTSTRAP_IP, BOOTSTRAP_PORT)

if LOCAL:
    address = BOOTSTRAP_IP
else:
    hostname = socket.gethostname()
    address = socket.gethostbyname(hostname)

if __name__ == '__main__':
    
    parser = ArgumentParser(description='Enter the cult...')
    
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional_arguments')

    required.add_argument('-p', type=int, help='port to listen on', required=True)
    optional.add_argument('-bs', action='store_true', help='set if the current node is the bootstrap')
    optional.add_argument('-c', help='consistency protocol, set if you want to use linearizability', default=EVENTUAL)
    optional.add_argument('-k', type=int, help='replication factor', default=1)

    args = parser.parse_args()
    port = args.p
    is_bootstrap = args.bs
    consistency = LINEARIZABILITY if args.c else EVENTUAL
    k = args.k
    
    b = "{}:{}".format(address, port).encode()
    ID = hashlib.sha1(b).hexdigest()
    print('Adding node: {}'.format(ID))

    node.IP = address
    node.port = port
    node.ID = ID
    
    timestamp = str(time())

    if is_bootstrap:
        node.k = k  # reqular nodes should be informed by bootstrap
        node.consistency = consistency
        node.create()
        with open('input/insert.txt') as f:
            lines = f.readlines()
        for line in lines:
            k, v = line.split(', ')
            v = v.split('\n')[0]
            enc = k.encode()
            hk = hashlib.sha1(enc).hexdigest()
            node.storage[hk] = (v, 1)
    else:
        node.join(BOOTSTRAP_ADDRESS, timestamp)
        
    app.run(host=address, port=port)
from config import *
import socket
import hashlib
import requests
import pickle
import time

from argparse import ArgumentParser
from flask import Flask, Blueprint, jsonify, request

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

    required.add_argument('-p', type=int, help='port to talk to', required=True)
    required.add_argument('-a', type=str, help='action', required=True)
    optional.add_argument('-k', type=str, help='song to look for/insert/delete')
    optional.add_argument('-v', type=str, help='value to insert')

    args = parser.parse_args()
    port = args.p
    action = args.a
    key = args.k
    value = args.v
    
    endpoint = 'http://' + address + ":" + str(port) + "/client/"
    
    try:
        if action == "search":
            if key == "*":
                endpoint += "overlay/values"
                response = requests.get(endpoint)
            else:
                endpoint += "search"
                response = requests.post(endpoint, data=pickle.dumps(key))
            print(response.text)
        elif action == "overlay":
            endpoint += "overlay/nodes"
            response = requests.get(endpoint)
            print(response.text)
        elif action == "insert":
            endpoint += "insert"
            response = requests.post(endpoint, data=pickle.dumps((key, (value, 1))))
            print(response.text)
        elif action == "delete":
            endpoint += "delete"
            response = requests.post(endpoint, data=pickle.dumps(key))
            print(response.text)
        elif action == "depart":
            endpoint += "depart"
            response = requests.post(endpoint)
            print(response.text)
    except:
        print('There was an error (probably no server listening to port {}).'.format(port))
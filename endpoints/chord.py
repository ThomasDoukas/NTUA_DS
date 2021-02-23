import requests
import pickle
import time
import hashlib
import threading
import asyncio
from Node import Node

from flask import Blueprint, jsonify, request

node = Node()

chord = Blueprint('chord', __name__)

@chord.route("/eureka", methods=['POST'])
def eureka():
    data = pickle.loads(request.get_data())
    if(data['overlay']):
        if(data['value']):
            node.ready = '\n'.join(data['node_list'])
        else:
            node.ready = ' -> '.join(data['node_list'])
        return node.ready
    if(data['search']):
        node.ready = '{}:{} -> {}'.format(data['succ_IP'], data['succ_port'], data['search'])
        return node.ready
    if(data['insert']):
        node.ready = '{}:{} -> ({}, {})'.format(data['succ_IP'], data['succ_port'], data['key'], data['value'])
        return node.ready
    if(data['delete']):
        if(not data['value']):
            node.ready = '{}:{} -> The requested key was not found.'.format(data['succ_IP'], data['succ_port'])
        else:
            node.ready = 'Record ({}, {}) deleted from {}:{} successfully!'.format(data['key'], data['value'], data['succ_IP'], data['succ_port'])
        return node.ready
    node.succ['ID'] = data['succ_ID']
    node.succ['IP'] = data['succ_IP']
    node.succ['port'] = data['succ_port']
    node.pred['ID'] = data['pred_ID']
    node.pred['IP'] = data['pred_IP']
    node.pred['port'] = data['pred_port']
    string = "Now I know who my successor is, I shall claim what is righteously mine!"
    print(string)
    node.request_items()
    node.notify_predecessor()
    return "200"


@chord.route('/join', methods=['POST'])
def join():
    print("/join")
    print(request)
    data = pickle.loads(request.get_data())
    ID = data['key']
    if ID in node.nodeDict.keys():
        print('Node already exists. Exiting operation.')
        return "400"
    
    node.nodeDict[ID] = data['node']
    data['overlay'] = False
    data['search'] = False
    data['insert'] = False
    data['delete'] = False
    data['node_list'] = []
    data['value'] = []
    # bisect.insort(node.nodeList, ID)
    node.find_successor(data)
    print("Forwarded query...")
    return "200"


@chord.route('/query', methods=['POST'])
def query():
    data = pickle.loads(request.get_data())
    ID = data['key']
    node.find_successor(data)
    return "200"


@chord.route('/notify', methods=['POST'])
def notify():
    data = pickle.loads(request.get_data())
    node.succ['ID'] = data['ID']
    node.succ['IP'] = data['IP']
    node.succ['port'] = data['port']
    print("Noted...")
    return "200"


@chord.route('/requestItems', methods=['POST'])
def requestItems():
    data = pickle.loads(request.get_data())
    node.pred['ID'] = data['ID']
    node.pred['IP'] = data['IP']
    node.pred['port'] = data['port']
    node.send_items()
    return "200"


@chord.route('/receiveItems', methods=['POST'])
def receiveItems():
    data = pickle.loads(request.get_data())
    for t in data['storage']:
        node.storage[t[0]] = t[1]
    print("Got them...")
    return "200"
    

@chord.route('/departure', methods=['POST'])
def departure():
    print("Ta pame.")
    data = pickle.loads(request.get_data())
    node.pred['ID'] = data['ID']
    node.pred['IP'] = data['IP']
    node.pred['port'] = data['port']
    for t in data['storage'].items():
        node.storage[t[0]] = t[1]
    node.notify_predecessor()
    return "200"

import requests
import pickle
from time import time
import hashlib
import asyncio

from flask import Blueprint, request
from endpoints.chord import node
from config import *

client = Blueprint('client', __name__)

@client.route('/client/search', methods=['POST'])
def search():
    data = pickle.loads(request.get_data())
    b = data.encode()
    hashedKey = hashlib.sha1(b).hexdigest()
    
    timestamp = str(time())
    args = {
        'dest_ID': node.ID,
        'dest_IP': node.IP,
        'dest_port': node.port,
        'key': hashedKey,
        'action': SEARCH,
        'consistency': node.consistency,
        'node_list': [],
        'value': {},
        'time': timestamp
    }
    endpoint = 'http://' + node.IP + ":" + str(node.port) + "/query"
    
    node.ready[timestamp] = ""
    async def barrier():
        while(not node.ready[timestamp]):
            pass
        return node.ready[timestamp]
    
    async def req():
        response = requests.post(endpoint, data=pickle.dumps(args))
        return response
    
    async def do():
        res2 = loop.create_task(req())
        res1 = loop.create_task(barrier())
        await asyncio.wait([res1, res2])
        return res1
    
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    res1 = loop.run_until_complete(do())
    loop.close()
    node.ready.pop(timestamp)
    return res1.result()


@client.route('/client/insert', methods=['POST'])
def insert():
    data = pickle.loads(request.get_data())
    b = data[0].encode()
    hashedKey = hashlib.sha1(b).hexdigest()
    
    timestamp = str(time())
    args = {
        'dest_ID': node.ID,
        'dest_IP': node.IP,
        'dest_port': node.port,
        'key': hashedKey,
        'action': INSERT,
        'consistency': node.consistency,
        'node_list': [],
        'value': {
            hashedKey : data[1]
        },
        'time': timestamp
    }
    endpoint = 'http://' + node.IP + ":" + str(node.port) + "/query"
    
    node.ready[timestamp] = ""
    async def barrier():
        while(not node.ready[timestamp]):
            pass
        return node.ready[timestamp]
    
    async def req():
        response = requests.post(endpoint, data=pickle.dumps(args))
        return response
    
    async def do():
        res2 = loop.create_task(req())
        res1 = loop.create_task(barrier())
        await asyncio.wait([res1, res2])
        return res1
    
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    res1 = loop.run_until_complete(do())
    loop.close()
    node.ready.pop(timestamp)
    return res1.result()


@client.route('/client/overlay/<mode>', methods=['GET'])
def overlay(mode):
    timestamp = str(time())
    if(mode == "nodes"):
        args = {
            'dest_ID': node.ID,
            'dest_IP': node.IP,
            'dest_port': node.port,
            'key': node.pred['ID'],
            'action': OVERLAY,
            'consistency': node.consistency,
            'node_list': [],
            'value': {},
            'time': timestamp
        }
    else:
        args = {
            'dest_ID': node.ID,
            'dest_IP': node.IP,
            'dest_port': node.port,
            'key': node.pred['ID'],
            'action': OVERLAY,
            'consistency': node.consistency,
            'node_list': [],
            'value': {'Not empty':'dictionary'},
            'time': timestamp
        }
    endpoint = 'http://' + node.IP + ":" + str(node.port) + "/query"
        
    node.ready[timestamp] = ""
    async def barrier():
        while(not node.ready[timestamp]):
            pass
        return node.ready[timestamp]
    
    async def req():
        response = requests.post(endpoint, data=pickle.dumps(args))
        return response
    
    async def do():
        res2 = loop.create_task(req())
        res1 = loop.create_task(barrier())
        await asyncio.wait([res1, res2])
        return res1
    
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    res1 = loop.run_until_complete(do())
    loop.close()
    node.ready.pop(timestamp)
    return res1.result()


@client.route('/client/delete', methods=['POST'])
def delete():
    data = pickle.loads(request.get_data())
    b = data.encode()
    hashedKey = hashlib.sha1(b).hexdigest()
    
    timestamp = str(time())
    args = {
        'dest_ID': node.ID,
        'dest_IP': node.IP,
        'dest_port': node.port,
        'key': hashedKey,
        'action': DELETE,
        'consistency': node.consistency,
        'node_list': [],
        'value': {},
        'time': timestamp
    }
    
    endpoint = 'http://' + node.IP + ":" + str(node.port) + "/query"
    
    node.ready[timestamp] = ""
    async def barrier():
        while(not node.ready[timestamp]):
            pass
        return node.ready[timestamp]
    
    async def req():
        response = requests.post(endpoint, data=pickle.dumps(args))
        return response
    
    async def do():
        res2 = loop.create_task(req())
        res1 = loop.create_task(barrier())
        await asyncio.wait([res1, res2])
        return res1
    
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    res1 = loop.run_until_complete(do())
    loop.close()
    node.ready.pop(timestamp)
    return res1.result()


@client.route('/client/depart', methods=['POST'])
def depart():
    timestamp = str(time())
    node.send_to_successor(timestamp)
    return 'Node {}:{} departed successfully!'.format(node.IP, node.port)

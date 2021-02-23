import requests
import pickle
import time
import hashlib
import threading
import asyncio

from flask import Blueprint, jsonify, request
from endpoints.chord import node

client = Blueprint('client', __name__)

@client.route('/client/search', methods=['POST'])
def search():
    data = pickle.loads(request.get_data())
    b = data.encode()
    hashedKey = hashlib.sha1(b).hexdigest()
    args = {
        'dest_ID': node.ID,
        'dest_IP': node.IP,
        'dest_port': node.port,
        'key': hashedKey,
        'overlay': False,
        'search': True,
        'insert': False,
        'delete': False,
        'node_list': [],
        'value': []
    }
    
    endpoint = 'http://' + node.IP + ":" + str(node.port) + "/query"
    
    async def thread_function():
        while(not node.ready):
            pass
        return node.ready
    
    async def req():
        response = requests.post(endpoint, data=pickle.dumps(args))
        return response
    
    async def do():
        res2 = loop.create_task(req())
        res1 = loop.create_task(thread_function())
        await asyncio.wait([res1, res2])
        return res1
    
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    res1 = loop.run_until_complete(do())
    loop.close()
    node.ready = []
    return res1.result()


@client.route('/client/insert', methods=['POST'])
def insert():
    data = pickle.loads(request.get_data())
    print(data)
    b = data[0].encode()
    hashedKey = hashlib.sha1(b).hexdigest()
    args = {
        'dest_ID': node.ID,
        'dest_IP': node.IP,
        'dest_port': node.port,
        'key': hashedKey,
        'overlay': False,
        'search': False,
        'insert': True,
        'delete': False,
        'node_list': [],
        'value': data[1]
    }
    
    endpoint = 'http://' + node.IP + ":" + str(node.port) + "/query"
    
    async def thread_function():
        while(not node.ready):
            pass
        return node.ready
    
    async def req():
        response = requests.post(endpoint, data=pickle.dumps(args))
        return response
    
    async def do():
        res2 = loop.create_task(req())
        res1 = loop.create_task(thread_function())
        await asyncio.wait([res1, res2])
        return res1
    
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    res1 = loop.run_until_complete(do())
    loop.close()
    node.ready = []
    return res1.result()


@client.route('/client/overlay/<mode>', methods=['GET'])
def overlay(mode):
    if(mode == "nodes"):
        args = {
            'dest_ID': node.ID,
            'dest_IP': node.IP,
            'dest_port': node.port,
            'key': node.pred['ID'],
            'overlay': True,
            'search': False,
            'insert': False,
            'delete': False,
            'node_list': [],
            'value': []
        }
    else:
        args = {
            'dest_ID': node.ID,
            'dest_IP': node.IP,
            'dest_port': node.port,
            'key': node.pred['ID'],
            'overlay': True,
            'search': False,
            'insert': False,
            'delete': False,
            'node_list': [],
            'value': ['indlovu']
        }
    endpoint = 'http://' + node.IP + ":" + str(node.port) + "/query"
        
    async def thread_function():
        while(not node.ready):
            pass
        return node.ready
    
    async def req():
        response = requests.post(endpoint, data=pickle.dumps(args))
        return response
    
    async def do():
        res2 = loop.create_task(req())
        res1 = loop.create_task(thread_function())
        await asyncio.wait([res1, res2])
        return res1
    
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    res1 = loop.run_until_complete(do())
    loop.close()
    node.ready = []
    return res1.result()


@client.route('/client/delete', methods=['POST'])
def delete():
    data = pickle.loads(request.get_data())
    b = data.encode()
    hashedKey = hashlib.sha1(b).hexdigest()
    args = {
        'dest_ID': node.ID,
        'dest_IP': node.IP,
        'dest_port': node.port,
        'key': hashedKey,
        'overlay': False,
        'search': False,
        'insert': False,
        'delete': True,
        'node_list': [],
        'value': []
    }
    
    endpoint = 'http://' + node.IP + ":" + str(node.port) + "/query"
    
    async def thread_function():
        while(not node.ready):
            pass
        return node.ready
    
    async def req():
        response = requests.post(endpoint, data=pickle.dumps(args))
        return response
    
    async def do():
        res2 = loop.create_task(req())
        res1 = loop.create_task(thread_function())
        await asyncio.wait([res1, res2])
        return res1
    
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    res1 = loop.run_until_complete(do())
    loop.close()
    node.ready = []
    return res1.result()


@client.route('/client/depart', methods=['POST'])
def depart():
    node.send_to_successor()
    return 'Node {}:{} departed successfully!'.format(node.IP, node.port)

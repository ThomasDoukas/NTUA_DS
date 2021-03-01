import requests
import pickle
import time
from time import time
import hashlib
import threading
import asyncio
from Node import Node
from config import *

from flask import Blueprint, jsonify, request

node = Node()

chord = Blueprint('chord', __name__)

@chord.route("/eureka", methods=['POST'])
def eureka():
    data = pickle.loads(request.get_data())
    if(data['action'] == OVERLAY):
        if(data['value']):
            node.ready[data['time']] = '\n'.join(data['node_list'])
        else:
            node.ready[data['time']] = ' -> '.join(data['node_list'])
        return node.ready[data['time']]
    elif(data['action'] == SEARCH):
        node.ready[data['time']] = '{}:{} -> {}'.format(data['succ_IP'], data['succ_port'], data['value'])
        return node.ready[data['time']]
    elif(data['action'] == INSERT):
        node.ready[data['time']] = '{}:{} -> ({}, {})'.format(data['succ_IP'], data['succ_port'], data['key'], data['value'][data['key']])
        return node.ready[data['time']]
    elif(data['action'] == DELETE):
        if(not data['value']):
            node.ready[data['time']] = '{}:{} -> The requested key was not found.'.format(data['succ_IP'], data['succ_port'])
        else:
            node.ready[data['time']] = 'Record ({}, {}) deleted from {}:{} successfully!'.format(data['key'], data['value'][data['key']], data['succ_IP'], data['succ_port'])
        return node.ready[data['time']]
    elif(data['action'] == JOIN):
        node.k = data['value']['k']
        node.succ['ID'] = data['succ_ID']
        node.succ['IP'] = data['succ_IP']
        node.succ['port'] = data['succ_port']
        node.pred['ID'] = data['pred_ID']
        node.pred['IP'] = data['pred_IP']
        node.pred['port'] = data['pred_port']
        print("Now I know who my successor is, I shall claim what is righteously mine!")
        '''
        -Receive replicas from predecessor
        -Receive records for which I am responsible (k==1) from successor (request->receive)
            -Replicate my whole storage and forward 
        * Sequence initiated with notify_predecessor needs to be completed first!
        '''
        timestamp = str(time())
        
        node.ready[timestamp] = ""
        if node.k == 1:
            node.notify_predecessor(timestamp)
            node.request_items(timestamp)
        else:
            async def barrier():
                while(not node.ready[timestamp]):
                    pass
                return node.ready[timestamp]
            
            async def req():
                node.notify_predecessor(timestamp)
                return "Notified..."
            
            async def do():
                res2 = loop.create_task(req())
                res1 = loop.create_task(barrier())
                await asyncio.wait([res1, res2])
                return res1
            
            asyncio.set_event_loop(asyncio.new_event_loop())
            loop = asyncio.get_event_loop()
            loop.run_until_complete(do())
            loop.close()
            print("NOW I CAN TALK TO MY SUCCESSOR AT LAST.")
            node.request_items(timestamp)
        node.ready.pop(timestamp)
        return "200"


@chord.route('/join', methods=['POST'])
def join():
    data = pickle.loads(request.get_data())
    data['value']['k'] = node.k
    node.find_successor(data)
    print("Forwarded query...")
    return "200"


@chord.route('/query', methods=['POST'])
def query():
    data = pickle.loads(request.get_data())
    
    repl = {}
    condition = (data['action'] == INS_REPL) or (data['action'] == DEL_REPL) # or data['insert'] or data['delete'] or data['join']
    fwd_to_self = (node.ID == data['dest_ID'])
    if(condition and (node.k > 1)):
        for t in data['value'].items():
            '''
            - If repl_num < k we need to increase and forward.
            - If repl_num == k we need to stop creating new replicas.
                However, there is a chance a new node has been assigned keys 
                for which their predecessor used to hold the last replica.
                In this case, we need to notify the previous tail, so as to delete any trailing replicas.
            '''
            if(not t[1]):
                '''
                If there's a new tail, delete local replica:
                    - ONLY IF THERE IS NO OVERLAP (i.e. the current node doesn't hold the record with replica_num == 1)
                '''
                if(data['action'] == INS_REPL):
                    try:
                        get = node.storage[t[0]]
                        if(get[1] == node.k):
                            node.storage.pop(t[0])
                    except:
                        pass
                elif(data['action'] == DEL_REPL):
                    try:
                        value = node.storage.pop(t[0])
                        if(value[1] < node.k):
                            repl[t[0]] = ()
                    except:
                        pass
            else:
                '''
                Overwite replica only if abs(new_replica_num - prev_replica_num) <= 1:
                We need to avoid overlaps (possible on joining).
                '''
                try:
                    gotIt = node.storage[t[0]]
                    if((abs(gotIt[1] - t[1][1]) <= 1) and (not fwd_to_self)):
                        node.storage[t[0]] = t[1]
                except:
                    node.storage[t[0]] = t[1]
                if(t[1][1] < node.k):
                    repl[t[0]] = (t[1][0], t[1][1] + 1)
                else:
                    repl[t[0]] = () # forward replica deletion message
        wena = data['time'].split('indlovu')
        if(len(wena) == 1):
            node.ready[wena[0]] = "Synchronised notify predecessor-receive from successor for replication purposes..."
        data['value'] = repl.copy()

    node.find_successor(data)
    return "200"


@chord.route('/notify', methods=['POST'])
def notify():
    data = pickle.loads(request.get_data())
    node.succ['ID'] = data['ID']
    node.succ['IP'] = data['IP']
    node.succ['port'] = data['port']
    print("Noted...")
    '''
    Someone entered in front of me:
        -Send everything to be replicated!
    '''
    repl = {}
    if(node.k > 1):
        for item in node.storage.items():
            if(item[1][1] < node.k):
                repl[item[0]] = (item[1][0], item[1][1] + 1)
    if repl:
        args = {
                'dest_ID': node.ID,
                'dest_IP': node.IP,
                'dest_port': node.port,
                'key': node.succ['ID'],
                'action': INS_REPL,
                'node_list': [],
                'value': repl,
                'time': data['time']
            }
        endpoint = 'http://' + node.succ['IP'] + ":" + str(node.succ['port']) + "/query"
        def thread_function():
            response = requests.post(endpoint, data=pickle.dumps(args))

        req = threading.Thread(target=thread_function, args=())
        req.start()
        return "Forwarded..."
    else:
        return "200"


@chord.route('/requestItems', methods=['POST'])
def requestItems():
    data = pickle.loads(request.get_data())
    node.pred['ID'] = data['ID']
    node.pred['IP'] = data['IP']
    node.pred['port'] = data['port']
    node.send_items(data['time'])
    return "200"


@chord.route('/receiveItems', methods=['POST'])
def receiveItems():
    data = pickle.loads(request.get_data())
    for t in data['storage'].items():
        node.storage[t[0]] = t[1]
    print("Got them...")
    repl = {}
    if(node.k > 1):
        '''
        That's alright, I'm already iterating over my whole storage.
        Fix synchronisation externally!!!
        '''
        for item in node.storage.items():
            if(item[1][1] < node.k):
                repl[item[0]] = (item[1][0], item[1][1] + 1)
    if repl:
        args = {
                'dest_ID': node.ID,
                'dest_IP': node.IP,
                'dest_port': node.port,
                'key': node.pred['ID'],
                'action': INS_REPL,
                'node_list': [],
                'value': repl,
                'time': data['time']
            }
        endpoint = 'http://' + node.succ['IP'] + ":" + str(node.succ['port']) + "/query"
        response = requests.post(endpoint, data=pickle.dumps(args))
        return response.text
    else:
        return "200"
    

@chord.route('/departure', methods=['POST'])
def departure():
    print("Ta pame.")
    data = pickle.loads(request.get_data())
    node.pred['ID'] = data['ID']
    node.pred['IP'] = data['IP']
    node.pred['port'] = data['port']
    node.notify_predecessor(data['time'])
    
    repl = {}
    if(node.k > 1):
        for t in data['storage'].items():
            try:
                got = node.storage[t[0]]
            except:
                got = ()
            if((not got) or (got[1] > t[1][1])):
                node.storage[t[0]] = t[1]
                if(t[1][1] < node.k):
                    repl[t[0]] = (t[1][0], t[1][1] + 1)
    if repl:
        data['dest_ID'] = node.ID
        data['dest_IP'] = node.IP
        data['dest_port'] = node.port
        data['key'] = node.pred['ID']
        data['action'] = INS_REPL
        data['node_list'] = []
        data['value'] = repl
        
        node.find_successor(data)
        
    return "200"

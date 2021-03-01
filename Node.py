import requests
import pickle
import hashlib
import threading
import time
import os
import signal
from config import *

class Node:
    
    def __init__(self):
        self.IP = None
        self.port = None
        self.ID = None
        self.succ = {
            'ID': None,
            'IP': None,
            'port': None
        }
        self.pred = {
            'ID': None,
            'IP': None,
            'port': None
        }
        self.storage = {}
        self.k = 1
        self.ready = {}
    
    ''' Finds successor. '''
    def find_successor(self, data):
        print("Searching for successor...")
        dest_ID = data['dest_ID']
        dest_IP = data['dest_IP']
        dest_port = data['dest_port']
        key = data['key']
        timeString = 'indlovu' + data['time']
        
        if(data['action'] == OVERLAY):
            if(data['value']):
                vault = '{}:{} :'.format(self.IP, self.port)
                for t in self.storage.items():
                    vault += "\n\t"
                    vault += '({}, {})'.format(t[0], t[1])
                data['node_list'].append(vault)
            else:
                data['node_list'].append('{}:{}'.format(self.IP, self.port))
        
        ''' Prepare to forward (address, endpoint, args)... '''
        address = 'http://' + '{}:{}'.format(self.succ['IP'], self.succ['port'])
        endpoint = '/query'
        args = {
            'dest_ID': dest_ID,
            'dest_IP': dest_IP,
            'dest_port': dest_port,
            'key': key,
            'action': data['action'],
            'node_list': data['node_list'],
            'value': data['value'],
            'time': data['time']
        }
        
        found = ()
        if(data['action'] == SEARCH):
            try:
                found = self.storage[key]
            except:
                pass
        if (between(self.pred['ID'], self.ID, key) or found):
            print("Node " + str(self.port) + " is the successor...")
            if(data['action'] == SEARCH):
                try:
                    data['value'] = self.storage[key]
                except:
                    data['value'] = "The requested key was not found."
            elif(data['action'] == INSERT):
                value = data['value'][key]
                self.storage[key] = value
                if(self.k > 1):
                    address = 'http://' + '{}:{}'.format(self.succ['IP'], self.succ['port'])
                    endpoint = '/query'
                    args = {
                        'dest_ID': self.ID,
                        'dest_IP': self.IP,
                        'dest_port': self.port,
                        'key': self.pred['ID'],
                        'action': INS_REPL,
                        'node_list': data['node_list'],
                        'value': {
                            data['key']: (value[0], value[1] + 1)
                        },
                        'time': timeString
                    }
                    def thread_function():
                        response = requests.post(address + endpoint, data=pickle.dumps(args))

                    req = threading.Thread(target=thread_function, args=())
                    req.start()
            elif(data['action'] == DELETE):
                try:
                    data['value'] = {
                        key: self.storage[key]
                    }
                    self.storage.pop(key)
                    if(self.k > 1):
                        address = 'http://' + '{}:{}'.format(self.succ['IP'], self.succ['port'])
                        endpoint = '/query'
                        args = {
                            'dest_ID': self.ID,
                            'dest_IP': self.IP,
                            'dest_port': self.port,
                            'key': self.pred['ID'],
                            'action': DEL_REPL,
                            'node_list': data['node_list'],
                            'value': {
                                data['key']: ()
                            },
                            'time': timeString
                        }
                        def thread_function():
                            response = requests.post(address + endpoint, data=pickle.dumps(args))

                        req = threading.Thread(target=thread_function, args=())
                        req.start()
                except:
                    data['value'] = {}
            elif((data['action'] == INS_REPL) or (data['action'] == DEL_REPL)):
                return "Enter the cult..."
            
            address = 'http://' + '{}:{}'.format(dest_IP, dest_port)
            endpoint = '/eureka'
            args = {
                'succ_ID': self.ID,
                'succ_IP': self.IP,
                'succ_port': self.port,
                'pred_ID': self.pred['ID'],
                'pred_IP': self.pred['IP'],
                'pred_port': self.pred['port'],
                'key': key,
                'action': data['action'],
                'node_list': data['node_list'],
                'value': data['value'],
                'time': data['time']
            }
        else:
            print("Forwarding to node " + str(self.succ['port']))

        def thread_function():
            response = requests.post(address + endpoint, data=pickle.dumps(args))

        req = threading.Thread(target=thread_function, args=())
        req.start()
    
    
    ''' Only gets run by the first node to join, as defined in paper. '''
    def create(self):
        print("Creating...")
        self.succ['ID'] = self.ID
        self.succ['IP'] = self.IP
        self.succ['port'] = self.port
        self.pred['ID'] = self.ID
        self.pred['IP'] = self.IP
        self.pred['port'] = self.port
    
    
    def join(self, BOOTSTRAP_ADDRESS, timestamp):
        print("Joining...")

        address = 'http://' + BOOTSTRAP_ADDRESS
        endpoint = '/join'
        args = {
            'dest_ID': self.ID,
            'dest_IP': self.IP,
            'dest_port': self.port,
            'key': self.ID,
            'action': JOIN,
            'node_list': [],
            'value': {},
            'time': timestamp
        }
        
        def thread_function():
            response = requests.post(address + endpoint, data=pickle.dumps(args))
            if(response.text == '400'):
                print("Node already exists, please try a different port.")
                os.kill(os.getpid(), signal.SIGINT)
                return 1

        req = threading.Thread(target=thread_function, args=())
        req.start()
        
        
    ''' We want to make sure we have the correct items in our storage. '''
    def request_items(self, timestamp):
        print("SEND THEM.")

        address = 'http://' + '{}:{}'.format(self.succ['IP'], self.succ['port'])
        endpoint = '/requestItems'
        args = {
            'ID': self.ID,
            'IP': self.IP,
            'port': self.port,
            'time': timestamp
        }

        def thread_function():
            response = requests.post(address + endpoint, data=pickle.dumps(args))

        req = threading.Thread(target=thread_function, args=())
        req.start()
    
    
    def notify_predecessor(self, timestamp):
        print("I AM YOUR FATHER.")

        address = 'http://' + '{}:{}'.format(self.pred['IP'], self.pred['port'])
        endpoint = '/notify'
        args = {
            'ID': self.ID,
            'IP': self.IP,
            'port': self.port,
            'time': timestamp
        }

        def thread_function():
            response = requests.post(address + endpoint, data=pickle.dumps(args))

        req = threading.Thread(target=thread_function, args=())
        req.start()


    ''' Sends relevant items to the node that claims to be your predecessor. '''
    def send_items(self, timestamp):
        send_dict = {}
        crap_dict = {}
        '''
            -Send those for which pred should be responsible (incidentally k == 1)
            -Don't send replicas, rely on pred->pred to do so, 
                    just delete those with replica_num == k
        '''
        for t in self.storage.items():
            if(not between(self.pred['ID'], self.ID, t[0]) and t[1][1] == 1):
                send_dict[t[0]] = t[1]
            ''' I am not the tail any more... '''
            if(t[1][1] == self.k and self.k > 1):
                crap_dict[t[0]] = t[1]
        
        ''' I am either not the tail or I am not responsible for the record any more... '''
        for t in send_dict.keys():
            self.storage.pop(t)
        for t in crap_dict.keys():
            self.storage.pop(t)

        print("Parta.")

        address = 'http://' + '{}:{}'.format(self.pred['IP'], self.pred['port'])
        endpoint = '/receiveItems'
        args = {
            'storage': send_dict,
            'time': timestamp
        }

        def thread_function():
            response = requests.post(address + endpoint, data=pickle.dumps(args))

        req = threading.Thread(target=thread_function, args=())
        req.start()
        
        
    '''Sends all items to successor before departure. '''
    def send_to_successor(self, timestamp):
        print("I'm out.")

        address = 'http://' + '{}:{}'.format(self.succ['IP'], self.succ['port'])
        endpoint = '/departure'
        args = {
            'storage': self.storage,
            'ID': self.pred['ID'],
            'IP': self.pred['IP'],
            'port': self.pred['port'],
            'time': timestamp
        }

        def thread_function():
            response = requests.post(address + endpoint, data=pickle.dumps(args))
            if(response.text == '200'):
                os.kill(os.getpid(), signal.SIGINT)

        req = threading.Thread(target=thread_function, args=())
        req.start()
                
def between(ID1, ID2, key):
    if ID1 == ID2:
        return True
    wrap = ID1 > ID2
    if not wrap:
        return True if key > ID1 and key <= ID2 else False
    else:
        return True if key > ID1 or  key <= ID2 else False

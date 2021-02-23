import requests
import pickle
import hashlib
import threading
import time
import os
import signal
from collections import OrderedDict

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
        self.nodeDict = OrderedDict()
        self.ready = False
    
    ''' Finds successor. '''
    def find_successor(self, data):
        print("Searching for successor...")
        dest_ID = data['dest_ID']
        dest_IP = data['dest_IP']
        dest_port = data['dest_port']
        key = data['key']
        if(data['overlay']):
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
            'overlay': data['overlay'],
            'search': data['search'],
            'insert': data['insert'],
            'delete': data['delete'],
            'node_list': data['node_list'],
            'value': data['value']
        }
        
        if (between(self.pred['ID'], self.ID, key)):
            print("Node " + str(self.port) + " is the successor...")
            if(data['search']):
                try:
                    data['search'] = self.storage[key]
                except:
                    data['search'] = "The requested key was not found."
            if(data['insert']):
                self.storage[key] = data['value']
            if(data['delete']):
                try:
                    data['value'] = self.storage[key]
                    self.storage.pop(key)
                except:
                    data['value'] = ""
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
                'overlay': data['overlay'],
                'search': data['search'],
                'insert': data['insert'],
                'delete': data['delete'],
                'node_list': data['node_list'],
                'value': data['value']
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
    
    
    def join(self, BOOTSTRAP_ADDRESS):
        print("Joining...")

        address = 'http://' + BOOTSTRAP_ADDRESS
        endpoint = '/join'
        args = {
            'dest_ID': self.ID,
            'dest_IP': self.IP,
            'dest_port': self.port,
            'key': self.ID,
            'node': self
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
    def request_items(self):
        print("SEND THEM.")

        address = 'http://' + '{}:{}'.format(self.succ['IP'], self.succ['port'])
        endpoint = '/requestItems'
        args = {
            'ID': self.ID,
            'IP': self.IP,
            'port': self.port
        }

        def thread_function():
            response = requests.post(address + endpoint, data=pickle.dumps(args))

        req = threading.Thread(target=thread_function, args=())
        req.start()
    
    
    def notify_predecessor(self):
        print("I AM YOUR FATHER.")

        address = 'http://' + '{}:{}'.format(self.pred['IP'], self.pred['port'])
        endpoint = '/notify'
        args = {
            'ID': self.ID,
            'IP': self.IP,
            'port': self.port
        }

        def thread_function():
            response = requests.post(address + endpoint, data=pickle.dumps(args))

        req = threading.Thread(target=thread_function, args=())
        req.start()


    ''' Sends relevant items to the node that claims to be your predecessor. '''
    def send_items(self):
        send_list = []
        for k, v in self.storage.items():
            if not between(self.pred['ID'], self.ID, k):
                send_list.append((k, v))
        for item in send_list:
            self.storage.pop(item[0])

        print("Parta.")

        address = 'http://' + '{}:{}'.format(self.pred['IP'], self.pred['port'])
        endpoint = '/receiveItems'
        args = {
            'storage': send_list
        }

        def thread_function():
            response = requests.post(address + endpoint, data=pickle.dumps(args))

        req = threading.Thread(target=thread_function, args=())
        req.start()
        
        
    '''Sends all items to successor before departure. '''
    def send_to_successor(self):
        print("I'm out.")

        address = 'http://' + '{}:{}'.format(self.succ['IP'], self.succ['port'])
        endpoint = '/departure'
        args = {
            'storage': self.storage,
            'ID': self.pred['ID'],
            'IP': self.pred['IP'],
            'port': self.pred['port']
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

def between_exclusive(ID1, ID2, key):
    if ID1 == ID2:
        return True
    wrap = ID1 > ID2
    if not wrap:
        return True if key > ID1 and key < ID2 else False
    else:
        return True if key > ID1 or  key < ID2 else False

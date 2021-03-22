import requests
import pickle
import threading
import os
import signal
from config import *

def between(ID1, ID2, key):
    if ID1 == ID2:
        return True
    wrap = ID1 > ID2
    if not wrap:
        return True if key > ID1 and key <= ID2 else False
    else:
        return True if key > ID1 or  key <= ID2 else False


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
        self.consistency = EVENTUAL
        self.ready = {}
    
    ''' 
    Finds successor. 
    This is one of the most crucial functions in the application,
    used by every major system action.
    It is based on the simple Chord principle for key searching:
        - if pred.ID < key <= self.ID then I'm responsible for the key (successor)
        - if not, the message is forwarded to my successor
    Depending on the type of system action (JOIN, SEARCH, INSERT, etc) different actions are triggered
    upon finding the successor node.
    In general, we use /eureka to terminate a find_successor sequence (either it is a client or a system action)
    and /query to forward the message to self.succ.
    '''
    def find_successor(self, data):
        print("Searching for successor...")
        dest_ID = data['dest_ID']
        dest_IP = data['dest_IP']
        dest_port = data['dest_port']
        succ_ID = self.ID
        succ_IP = self.IP
        succ_port = self.port
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
            'consistency': data['consistency'],
            'node_list': data['node_list'],
            'value': data['value'],
            'time': data['time']
        }
        
        found = ()
        if((data['action'] == SEARCH) and ((data['consistency'] == EVENTUAL) or (data['consistency'] == LINEARIZABILITY))):
            try:
                found = self.storage[key]
            except:
                pass
            
        if (between(self.pred['ID'], self.ID, key) or found):
            print("Node " + str(self.port) + " is the successor...")
            if(data['action'] == SEARCH):
                if((data['consistency'] == LINEARIZABILITY) and (self.k > 1)):
                    '''
                        We have found a replica manager.
                        Now we need to forward the query until we reach the tail.
                        To differentiate between the two phases of the linearizability search we will use a different flag.
                        We also need to update the key with the successor's ID, 
                        so that each one of my successors (and possible secondary replica managers)
                        is potentially able to return to the client with the query response.
                    '''
                    if (found):
                        if (found[1] < self.k):
                            succ_ID = self.succ['ID']
                            succ_IP = self.succ['IP']
                            succ_port = self.succ['port']
                        address = 'http://' + '{}:{}'.format(succ_IP, succ_port)
                        endpoint = '/query'
                        args = {
                            'dest_ID': dest_ID,
                            'dest_IP': dest_IP,
                            'dest_port': dest_port,
                            'key': succ_ID,
                            'action': SEARCH,
                            'consistency': LINEARIZABILITY_PHASE_2,
                            'node_list': data['node_list'],
                            'value': {
                                data['key']: found
                            },
                            'time': data['time']
                        }
                        def thread_function():
                            requests.post(address + endpoint, data=pickle.dumps(args))

                        req = threading.Thread(target=thread_function, args=())
                        req.start()
                        return "Enter the cult..."
                    
                    else:
                        data['value'] = "The requested key was not found."

                elif (data['consistency'] == LINEARIZABILITY_PHASE_2):
                    '''
                        We are looking for the tail.
                        The tail is:
                            1 - either the node with replica_num == k (replication factor)
                            2 - or the node that first discovers that the replica_num forwarded to them
                                is greater than the locally stored one. That can happen if the total number 
                                of participating nodes is smaller than the replication factor (circle).
                        We need to check whether the current node is the tail.
                        If it is not:
                            - forward to the successor, using their ID as the (search) key, attaching the locally stored replica 
                        If it is:
                            - return to the client using:
                                1 - the locally stored replica
                                2 - the replica forwarded to me                    
                    '''
                    replKey, val = next(iter((data['value'].items()))) # Get first (and only) key-value pair of the dictionary
                    replica = self.storage[replKey]
                    if (val[1] > replica[1]):
                        data['value'] =  val
                        succ_ID = self.pred['ID']
                        succ_IP = self.pred['IP']
                        succ_port = self.pred['port']
                    elif ((replica[1] == self.k) or (self.ID == self.succ['ID'])):
                        data['value'] =  replica
                    else:
                        address = 'http://' + '{}:{}'.format(self.succ['IP'], self.succ['port'])
                        endpoint = '/query'
                        args = {
                            'dest_ID': dest_ID,
                            'dest_IP': dest_IP,
                            'dest_port': dest_port,
                            'key': self.succ['ID'],
                            'action': SEARCH,
                            'consistency': LINEARIZABILITY_PHASE_2,
                            'node_list': data['node_list'],
                            'value': {
                                replKey: replica
                            },
                            'time': data['time']
                        }
                        def thread_function():
                            requests.post(address + endpoint, data=pickle.dumps(args))

                        req = threading.Thread(target=thread_function, args=())
                        req.start()
                        return "Enter the cult..."
                else:
                    try:
                        data['value'] = self.storage[key]
                    except:
                        data['value'] = "The requested key was not found."

            elif(data['action'] == INSERT):
                value = data['value'][key]
                self.storage[key] = value
                if(self.k > 1 and (self.ID != self.succ['ID'])):
                    address = 'http://' + '{}:{}'.format(self.succ['IP'], self.succ['port'])
                    endpoint = '/query'
                    args = {
                        'dest_ID': dest_ID,
                        'dest_IP': dest_IP,
                        'dest_port': dest_port,
                        'key': self.succ['ID'],
                        'action': INS_REPL,
                        'consistency': data['consistency'],
                        'node_list': [],
                        'value': {
                            data['key']: (value[0], value[1] + 1)
                        },
                        'time': timeString
                    }
                    def thread_function():
                        requests.post(address + endpoint, data=pickle.dumps(args))

                    req = threading.Thread(target=thread_function, args=())
                    req.start()
                    
                    ''' 
                        - linearizability: the tail returns to the client -> NO EUREKA
                        - eventual: the primary returns immediately after the insertion -> EUREKA
                    '''
                    if (data['consistency'] == LINEARIZABILITY):
                        return "Enter the cult..."

            elif(data['action'] == DELETE):
                try:
                    data['value'] = {
                        key: self.storage[key]
                    }
                    self.storage.pop(key)
                    if(self.k > 1 and (self.ID != self.succ['ID'])):
                        address = 'http://' + '{}:{}'.format(self.succ['IP'], self.succ['port'])
                        endpoint = '/query'
                        args = {
                            'dest_ID': dest_ID,
                            'dest_IP': dest_IP,
                            'dest_port': dest_port,
                            'key': self.succ['ID'],
                            'action': DEL_REPL,
                            'consistency': data['consistency'],
                            'node_list': [],
                            'value': {
                                data['key']: ()
                            },
                            'time': timeString
                        }
                        def thread_function():
                            requests.post(address + endpoint, data=pickle.dumps(args))

                        req = threading.Thread(target=thread_function, args=())
                        req.start()
                        
                        ''' Same as Insert '''
                        if (data['consistency'] == LINEARIZABILITY):
                            return "Enter the cult..."
                        
                except:
                    data['value'] = {}

                '''
                In the cases of STOP_INS and STOP_DEL:
                We simply need to prepare the arguments to hit /eureka.
                    node_list contains a list of tuples with the version history:
                        t[0]: the node that made an insertion/deletion
                        t[1]: the key-value pair inserted/deleted
                    We need to fetch the last tuple in the list (node_list[-1]),
                    which contains the tail of the insertion/deletion, as well the corresponding key-value pair.
                '''
            elif((data['action'] == STOP_INS)):
                data['action'] = INSERT
                succ_ID = data['node_list'][-1][0]['ID']
                succ_IP = data['node_list'][-1][0]['IP']
                succ_port = data['node_list'][-1][0]['port']
                '''
                Now a different thread has to forward the deletion message to the successor.
                We don't care about most of the arguments below.
                It is important that 'value' gets the deletion message sent via data['value'].
                'action' could be either INS_REPL or just REPL.
                '''
                if (self.ID != self.succ['ID']):
                    address = 'http://' + '{}:{}'.format(self.succ['IP'], self.succ['port'])
                    endpoint = '/query'
                    args = {
                        'dest_ID': dest_ID,
                        'dest_IP': dest_IP,
                        'dest_port': dest_port,
                        'key': self.succ['ID'],
                        'action': INS_REPL,
                        'consistency': data['consistency'],
                        'node_list': data['node_list'],
                        'value': data['value'],
                        'time': data['time'] #timeString
                        # 'time': data['time'].split('indlovu')[1]
                    }
                    def thread_function():
                        requests.post(address + endpoint, data=pickle.dumps(args))

                    req = threading.Thread(target=thread_function, args=())
                    req.start()
                
                key = data['node_list'][-1][1]['key']
                try:
                    data['time'] = data['time'].split('indlovu')[1]
                except:
                    pass
                data['value'] = {
                    key: data['node_list'][-1][1]['value']
                }
                
            elif((data['action'] == STOP_DEL)):
                data['action'] = DELETE
                succ_ID = data['node_list'][-1][0]['ID']
                succ_IP = data['node_list'][-1][0]['IP']
                succ_port = data['node_list'][-1][0]['port']
                
                key = data['node_list'][-1][1]['key']
                try:
                    data['time'] = data['time'].split('indlovu')[1]
                except:
                    pass
                data['value'] = {
                    key: data['node_list'][-1][1]['value']
                }

            elif((data['action'] == INS_REPL) or (data['action'] == DEL_REPL) or (data['action'] == REPL)):
                return "Enter the cult..."
            
            address = 'http://' + '{}:{}'.format(dest_IP, dest_port)
            endpoint = '/eureka'
            args = {
                'succ_ID': succ_ID,
                'succ_IP': succ_IP,
                'succ_port': succ_port,
                'pred_ID': self.pred['ID'],
                'pred_IP': self.pred['IP'],
                'pred_port': self.pred['port'],
                'key': key,
                'action': data['action'],
                'consistency': data['consistency'],
                'node_list': data['node_list'],
                'value': data['value'],
                'time': data['time']
            }
        else:
            print("Forwarding to node " + str(self.succ['port']))

        def thread_function():
            requests.post(address + endpoint, data=pickle.dumps(args))

        req = threading.Thread(target=thread_function, args=())
        req.start()
    
    
    ''' Only gets run by the bootstrap node. '''
    def create(self):
        print("Creating...")
        self.succ['ID'] = self.ID
        self.succ['IP'] = self.IP
        self.succ['port'] = self.port
        self.pred['ID'] = self.ID
        self.pred['IP'] = self.IP
        self.pred['port'] = self.port
    
    ''' Gets run by every node other than the bootstrap when starting the server. '''
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
        
        
    ''' 
    When a new node enters the ring, after it finds its successor, 
    it asks for any items (key-value pair) for which it is now responsible (pred.ID < key <= self.ID)
    '''
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
            requests.post(address + endpoint, data=pickle.dumps(args))

        req = threading.Thread(target=thread_function, args=())
        req.start()
    
    ''' 
    When a new node enters the ring, after it finds its successor, 
    it notifies its predecessor so that the latter can update their succ info 
    and send out any item that needs to be replicated.
    '''
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
            requests.post(address + endpoint, data=pickle.dumps(args))

        req = threading.Thread(target=thread_function, args=())
        req.start()


    ''' Sends relevant items to the newly inserted node that is now my predecessor. '''
    def send_items(self, timestamp):
        send_dict = {}
        crap_dict = {}
        '''
            -Send those for which pred should be responsible (incidentally those with k == 1)
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
            requests.post(address + endpoint, data=pickle.dumps(args))

        req = threading.Thread(target=thread_function, args=())
        req.start()
        
        
    ''' Sends all items to successor before departure. '''
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

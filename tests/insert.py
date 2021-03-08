import os
import requests
import pickle
import random
from argparse import ArgumentParser
from time import time

NODES = {
    '1': '127.0.0.1:5000',
    '2': '127.0.0.1:5001',
    '3': '127.0.0.1:5002',
    '4': '127.0.0.1:5003',
    '5': '127.0.0.1:5004',
    '6': '127.0.0.1:5005',
    '7': '127.0.0.1:5006',
    '8': '127.0.0.1:5007',
    '9': '127.0.0.1:5008',
    '10': '127.0.0.1:5009'
}

def get_node():
    rnd = random.randint(1, 10)
    return NODES[str(rnd)]

if __name__ == '__main__':
    
    parser = ArgumentParser(description='Enter the cult...')
    
    required = parser.add_argument_group('required arguments')

    required.add_argument('-k', type=str, help='replication factor', required=True)
    required.add_argument('-c', type=str, help='consistency', required=True)

    args = parser.parse_args()
    repl = args.k
    consistency = args.c
    
    ops = 0
    
    if not os.path.exists('tests/output'):
        os.makedirs('tests/output')
    if not os.path.exists('tests/output/insert'):
        os.makedirs('tests/output/insert')
        
    if not os.path.exists('tests/throughput'):
        os.makedirs('tests/throughput')
    
    out = open('tests/output/insert/'+ consistency + '_' + repl + '.out', 'a')
    with open('tests/input/insert.txt', 'r') as fin:
        lines = fin.readlines()
        
    start_time = time()
    for line in lines:
        ops += 1
        k, v = line.split(', ')
        v = v[:-1]
        endpoint = 'http://' + get_node() + "/client/insert"
        response = requests.post(endpoint, data=pickle.dumps((k, (v, 1))))
        out.write(response.text + '\n')

    end_time = time()
    elapsed_time = end_time - start_time
    thr = elapsed_time/ops
    
    with open('tests/throughput/insert.out', 'a') as f:
        f.write("Consistency: " + consistency + " Replication Factor: " + repl + " Write Throughput: " + str(thr) + '\n')
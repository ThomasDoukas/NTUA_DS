from config import *
import socket
import requests
import pickle
import os

from argparse import ArgumentParser
from PyInquirer import style_from_dict, Token, prompt

BOOTSTRAP_ADDRESS = "{}:{}".format(BOOTSTRAP_IP, BOOTSTRAP_PORT)

if LOCAL:
    address = BOOTSTRAP_IP
else:
    hostname = socket.gethostname()
    address = socket.gethostbyname(hostname)


style = style_from_dict({
    Token.QuestionMark: '#E91E63 bold',
    Token.Selected: '#673AB7 bold',
    Token.Instruction: '#0bf416',  # default
    Token.Answer: '#2196f3 bold',
    Token.Question: '#0bf416 bold',
})

def red(string):
    return '\033[1;91m{}\033[00m'.format(string)

def yellow(string):
    return "\033[1;93m{}\033[00m\n".format(string)

def Home():
    Home_q = [
        {
            'type': 'list',
            'name': 'option',
            'message': 'Press Enter to return to the Home Menu.',
            'choices': ['Exit'],
            'filter': lambda val: val.lower()
        }]
    Home_a = prompt(Home_q)['option']
    return Home_a

def client(port):
    os.system('cls||clear')
    yellow('What a beautiful day to enter the cult...')
    while True:
        print("----------------------------------------------------------------------")
        method_q = [
            {
                'type': 'list',
                'name': 'method',
                'message': 'What would you like to do?',
                'choices': ['Network Overlay', 'Search for a Song', 'Insert a Song', 'Delete a Song', 'Depart', 'Help', 'Exit']
            }]
        method_a = prompt(method_q, style=style)["method"]
        os.system('cls||clear')
        if method_a == 'Search for a Song':
            print("Search for a Song")
            print("----------------------------------------------------------------------")
            endpoint = 'http://' + address + ":" + str(port) + "/client/"
            search_q = [
                {
                    'type': 'input',
                    'name': 'song',
                    'message': 'Song to look for:',
                    'filter': lambda val: str(val)
                }]
            search_a = prompt(search_q, style=style)
            search_confirm_a = True #prompt(search_confirm_q)["search_confirm"]
            if search_confirm_a:
                if search_a["song"] == "*":
                    endpoint += "overlay/values"
                    response = requests.get(endpoint)
                else:
                    endpoint += "search"
                    key = search_a["song"]
                    response = requests.post(endpoint, data=pickle.dumps(key))
                print(yellow(response.text))
                continue

        elif method_a == 'Network Overlay':
            print("Overlay of Nodes currently in the Chord Ring.")
            print("----------------------------------------------------------------------\n")
            endpoint = 'http://' + address + ":" + str(port) + "/client/"
            endpoint += "overlay/nodes"
            response = requests.get(endpoint)
            print(yellow(response.text))
            continue

        elif method_a == 'Insert a Song':
            print("Insert a new song")
            print("----------------------------------------------------------------------")
            endpoint = 'http://' + address + ":" + str(port) + "/client/"
            insert_q = [
                {
                    'type': 'input',
                    'name': 'song insert',
                    'message': 'Song to insert to the Chord Ring:',
                    'filter': lambda val: str(val)
                },
                {
                    'type': 'input',
                    'name': 'value',
                    'message': 'Value:',
                    'filter': lambda val: str(val)
                }]
            insert_a = prompt(insert_q, style=style)
            print("\nConfirm insertion:")
            insert_confirm_q = [
                {
                    'type': 'confirm',
                    'name': 'insert_confirm',
                    'message': 'Add ' + insert_a["song insert"] + ' with value ' + insert_a["value"] + ' to storage?',
                    'default': True
                }
            ]
            insert_confirm_a = prompt(insert_confirm_q)["insert_confirm"]
            if insert_confirm_a:
                endpoint += "insert"
                key = insert_a["song insert"]
                value = insert_a["value"]
                response = requests.post(endpoint, data=pickle.dumps((key, (value, 1))))
                print(yellow(response.text))
                continue

        elif method_a == 'Delete a Song':
            print("Delete a song")
            print("----------------------------------------------------------------------")
            endpoint = 'http://' + address + ":" + str(port) + "/client/"
            delete_q = [
                {
                    'type': 'input',
                    'name': 'song delete',
                    'message': 'Song to delete from the Chord Ring:',
                    'filter': lambda val: str(val)
                }]
            delete_a = prompt(delete_q, style=style)
            print("\nConfirm deletion:")
            delete_confirm_q = [
                {
                    'type': 'confirm',
                    'name': 'delete_confirm',
                    'message': 'Are you sure you want to delete the song ' + delete_a["song delete"] + ' from storage?',
                    'default': False
                }
            ]
            delete_confirm_a = prompt(delete_confirm_q)["delete_confirm"]
            if delete_confirm_a:
                endpoint += "delete"
                key = delete_a["song delete"]
                response = requests.post(endpoint, data=pickle.dumps(key))
                print(yellow(response.text))
                continue

        elif method_a == 'Depart':
            print("Departing from the Chord Ring.")
            print("----------------------------------------------------------------------\n")
            endpoint = 'http://' + address + ":" + str(port) + "/client/"
            endpoint += "depart"
            response = requests.post(endpoint)
            print(yellow(response.text))
            break
        
        elif method_a == 'Help':
            print("This is the manual page for the CLI tool.")
            print("--------------------------------------------------------------------------\n")
            print(yellow("Use the arrow keys to navigate and 'Enter' to select.\nAvoid clicking anywhere inside the CLI (it could terminate the program)."))
            print(red("Network Overlay:"))
            print(yellow(" Select this option to get a view of the Chord overlay.\n " +\
                "The addresses of all participating nodes will be printed."))
            print(red("Search for a Song:"))
            print(yellow(" Select this option to make a query to the Chord Ring.\n " +\
                "You will be prompted to insert a song title to look for.\n " +\
                "Insert '*' to get all songs stored in each node."))
            print(red("Insert a Song:"))
            print(yellow(" Select this option to insert a song to the application.\n " +\
                "You will be prompted to provide the title of the song (key),\n " +\
                "as well as a string signifying the location of the actual file (value)."))
            print(red("Delete a Song:"))
            print(yellow(" Select this option to delete all replicas of a specific song from Chord.\n " +\
                "You will be prompted to provide the title of a song to delete."))
            print(red("Depart:"))
            print(yellow(" Select this option to make the current node gracefully depart.\n " +\
                "Before departing, the node sends all of its keys to the successor."))
            print(red("Exit:"))
            print(yellow(" Select this option to exit the CLI tool."))
            print("--------------------------------------------------------------------------\n")
            
            home_q = [
                {
                    'type': 'confirm',
                    'name': 'home',
                    'message': 'Would you like to return to the Home Menu?',
                    'default': False
                }
            ]
            
            while(not prompt(home_q)["home"]):
                print(red("  Why would you even press Enter in the first place."))
                pass
            os.system('cls||clear')
            continue
            # if Home() == 'exit':
            #     os.system('cls||clear')
            #     continue
            
        elif method_a == 'Exit':
            os.system('cls||clear')
            break

        else:
            break



if __name__ == '__main__':
    
    parser = ArgumentParser(description='Enter the cult...')
    
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional_arguments')

    required.add_argument('-p', type=int, help='port to talk to', required=True)

    args = parser.parse_args()
    port = args.p
    
    client(port)
# Server

import socket
import threading
import concurrent.futures

from protocol import send_message, recv_message
from storage import KeyValueStore, TaskQueue


class Server:

    def __init__(self):
        # Create the socket, the worker pool, the storage, and the task queue
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        self.storage = KeyValueStore()
        self.task_queue = TaskQueue()

    def start_server(self):
        # Starts the server by assigning the IP and port, then leaves the socket to listen for incoming connections.
        self.server_sock.bind(('0.0.0.0', 9090))
        self.server_sock.listen()
        # Printout according the assingment spec. __init__ could theoretically take a max workers arguement,
        # and start_server could take a tuple for the binding, but since all values are hard coded this works.
        print(f'[*] Server initialized on 0.0.0.0:9090' f'\n'
              f'[*] Thread Pool initialized with max_workers=10...')

    def accept_client(self):
        # Will loop as well. Will accept the connection, assign a worker, and call process client.
        while True:
            # This should probably have try/except logic for the errors that may come up.
            client_sock, client_addr = self.server_sock.accept()
            self.executor.submit(self.process_client, client_sock)

    def process_client(self, client):
        # Loop needs to do a few things. Take a message. Process the request, perform the request, then respond
        try:
            while True:
                # Remember, recv_message() is written to take a dictionary according to the assignment.
                inbound = recv_message(client)
                outbound = self.process_request(inbound)
                send_message(client, outbound)
        except ConnectionError:
            # Handles the connection error that protocol.py throws when theres no incoming data.
            print(f'Conncection from client socket {client} closed')
        except RecursionError:
            # I'm gonna throw some really long junk test messages just to feel validated in the error handling
            # and design choices.
            print(f'Your stress test actually broke something.')
        finally:
            client.close()

    def process_request(self, message):

        try:
            action = message.get('action')
            assert action == any(['PUT', 'GET', 'ENQUEUE', 'DEQUEUE'])
            # Determine and perform the action.
            if action == "GET":
                return self.storage.get(message['key'])
            elif action == "PUT":
                return self.storage.put(message['key'], message['value'])
            elif action == 'ENQUEUE':
                return self.task_queue.enqueue(message['task'])
            elif action == 'DEQUEUE':
                return self.task_queue.dequeue()
        except KeyError:
            # Deals with the KeyError that KeyValueStorage.get() can raise.
            return f'Key {message['key']} does not exist.'
        except AssertionError:
            # Action can be None in two ways, one in the assignment if 'action' isn't a key in message, or two,
            # None is the value mapped to action. I can't differentiate the two without needing to customize the
            # KeyError more. Regardless, the result would be the same.
            return f'{action} is not a valid action.'


# A flag to distinguish between testing or not. Set to true to see use the code that tests the server implementation
testing = True

if __name__ == '__main__':
    server = Server()
    server.start_server()

    client = socket.socket()
    test_dict1 = {'action': 'PUT', 'key': 'Name', 'value': 'First Name'}
    test_dict2 = {'action': 'GET', 'key': 'Name'}
    test_dict3 = {'action': 'ENQUEUE', 'task': {'email': ''}}
    test_dict4 = {'action': 'DEQUEUE'}

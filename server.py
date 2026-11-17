# Server

import socket
import threading
import concurrent.futures

from protocol import *
from storage import KeyValueStore, TaskQueue


class Server:

    def __init__(self):
        # Create the socket, the worker pool, the storage, and the task queue
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        self.storage = KeyValueStore()
        self.task_queue = TaskQueue()
        self.interface = threading.Thread(target=self.server_interface)
        self.server = threading.Thread(target=self.accept_client)
        self.shutdown_event = threading.Event()

    def start_server(self):
        # Starts the server by assigning the IP and port, then leaves the socket to listen for incoming connections.
        self.server_sock.bind(('0.0.0.0', 9090))
        self.server_sock.listen()

        # Printout according the assingment spec. __init__ could theoretically take a max workers arguement,
        # and start_server could take a tuple for the binding, but since all values are hard coded this works.
        print(f'[*] Server initialized on 0.0.0.0:9090' f'\n'
              f'[*] Thread Pool initialized with max_workers=10...')

        # Actually start running things
        self.interface.start()
        self.server.start()
        self.interface.join()
        self.server.join()
        self.executor.shutdown(wait=True)

    def accept_client(self):
        # Will loop as well. Will accept the connection, assign a worker, and call process client.
        try:
            while not self.shutdown_event.is_set():
                # This should probably have try/except logic for the errors that may come up.
                client_sock, client_addr = self.server_sock.accept()
                self.executor.submit(self.process_client, client_sock)
                print(f'accepted client {client_sock}')
        except OSError:
            # Since we're using the bash cli provided by the dev container/vscode, I wanted a way to shutdown the
            # server without needing to kill the terminal. The way the threads are set up makes it so that when
            # interface sees the command, the event is set and then the logic for server_interface() ends, causing
            # the thread to finish. The accept thread is blocked by server.sock.accept(), which means it won't see
            # the event. So we just shoot the socket on the way out, and catch the error.
            if self.shutdown_event.is_set():
                pass
            else:
                raise

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
            assert 'action' in message
            action = message.get('action')
            assert action in ['PUT', 'GET', 'ENQUEUE', 'DEQUEUE']
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
            if 'action' in message:
                return f'{action} is not a valid action.'
            else:
                return f'No action field.'

    def server_interface(self):
        while not self.shutdown_event.is_set():
            command = input("Server command: ")

            if command.lower() == "shutdown":
                self.shutdown_event.set()
        # This throws an OS error accept needs to handle
        self.server_sock.close()


# A flag to distinguish between testing or not. Set to true to see use the code that tests the server implementation
testing = True


def testing():
    client = socket.socket()
    test_dict1 = {'action': 'PUT', 'key': 'Name', 'value': 'First Name'}
    test_dict2 = {'action': 'GET', 'key': 'Name'}
    test_dict3 = {'action': 'ENQUEUE', 'task': {'email': ''}}
    test_dict4 = {'action': 'DEQUEUE'}

    client.connect(('0.0.0.0', 9090))

    send_message(client, test_dict1)
    send_message(client, test_dict2)
    send_message(client, test_dict3)
    send_message(client, test_dict4)


if __name__ == '__main__':
    server = Server()
    server.start_server()

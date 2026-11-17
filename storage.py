# Provides the requirements for thread-safe key-value storage and a conditioned based task queue.

import threading
# Not needed for assignment functionality, here for testing purposes.
import time
import random


class KeyValueStore:
    # Provides thread safe storage. Put and get are the required functionality required from outside,
    # simply either writing a key-value pair to the dictionary or get a value from a key (or letting an error
    # propogate if not). Keeping the data thread safe is handled predominantly by the admit function.
    def __init__(self):
        # Sets up a mutex and a blank dictionary to use.
        self.store = {}
        self.lock = threading.Condition()
        # These support the conditions needed for admission logic.
        self.requests = []
        self.next_req_id = 0
        self.active_readers = 0
        self.active_writer = False

    def put(self, key, value):
        # Keeps ID's unique and thread safe
        with self.lock:
            request_id = self.next_req_id
            self.next_req_id += 1
        # Prepares the request
        self.admit(request_id, 'write')
        # Completes the request
        try:
            self.store[key] = value
            return f'Stored key: {key} with value: {value}'
        except Exception:
            # Skimming the docs for dictionaries (and subsequently hashable), using a non-hashable key should be
            # the only error that can be user created.
            return f'Failed to store key: {key} with value: {value}'
        finally:
            self.release_write()

    def get(self, key):
        # Keeps the ID's unqiue and thread safe
        with self.lock:
            request_id = self.next_req_id
            self.next_req_id += 1
        # Prepares the request
        self.admit(request_id, 'read')

        # completes the request.
        # NOTE: Throws an error if the key doesn't exist. Still allows a key have value None. Exception logic
        # should be handled by whatever is calling the method.
        try:
            value = self.store[key]
        finally:
            self.release_read()
        return value

    def admit(self, request_id, request_type):
        # Handles request processing in a thread safe manner. The logic uses a FIFO-queue sctructure, taking requests,
        # and then admitting them as possible. The logic allows for consequetive read requests to be proccessed
        # concurrently, and writers given an exclusive lock.
        with self.lock:
            # Add request to waiting queue
            self.requests.append((request_id, request_type))

            while True:
                # Find this request's position in the queue
                position = self.requests.index((request_id, request_type))

                if request_type == 'read':
                    # Check whether a writer is ahead of this reader.
                    writer_ahead = any(kind == 'write' for _,
                                       kind in self.requests[:position])

                    # If there is no active writer and no writer ahead of the read, execute the read.
                    if not self.active_writer and not writer_ahead:
                        self.requests.pop(position)
                        self.active_readers += 1
                        self.lock.notify_all()
                        return

                elif request_type == 'write':
                    # If there are no active readers, active writers, and this is the next request, execute the write
                    if position == 0 and not self.active_writer and self.active_readers == 0:
                        self.requests.pop(0)
                        self.active_writer = True
                        self.lock.notify_all()
                        return

                self.lock.wait()

    def release_read(self):
        with self.lock:
            self.active_readers -= 1
            self.lock.notify_all()

    def release_write(self):
        with self.lock:
            self.active_writer = False
            self.lock.notify_all()


class TaskQueue:

    def __init__(self):
        self.queue = []
        self.lock = threading.Condition()

    def dequeue(self):
        with self.lock:
            while not self.queue:
                self.lock.wait()
            return self.queue.pop(0)

    def enqueue(self, task):
        with self.lock:
            self.queue.append(task)
            self.lock.notify()
            return f'Task {task} added to the queue.'

################################ Not needed for assignment deliverables #######################################

# Helper to test the admissions.


def reader_test(request_id, request_type):
    storage.admit(request_id, request_type)
    print(f"Thread {request_id, request_type} entered")

    time.sleep(random.randint(1, 5))
    if request_type == 'read':
        storage.release_read()
    else:
        storage.release_write()
    print(f"Thread {request_id, request_type} exited")


if __name__ == '__main__':
    print(f'Running test for storage.py')

    storage = KeyValueStore()

    # Testing KeyValueStore functionality
    storage.put("Name", "First Name")
    assert storage.get("Name") == 'First Name'
    storage.put("Name", None)
    assert storage.get("Name") == None

    try:
        storage.get("Age")
    except KeyError:
        pass
    else:
        assert False, 'Missing key'

    # Confirming admission logic. The first two readers should enter before either exits. Then the first write should
    # exit before the second one enters. Then the final read.
    reader1 = threading.Thread(target=reader_test, args=(1, 'read'))
    reader2 = threading.Thread(target=reader_test, args=(2, 'read'))
    writer1 = threading.Thread(target=reader_test, args=(3, 'write'))
    writer2 = threading.Thread(target=reader_test, args=(4, 'write'))
    reader3 = threading.Thread(target=reader_test, args=(5, 'read'))

    reader1.start()
    reader2.start()
    writer1.start()
    writer2.start()
    reader3.start()

    reader1.join()
    reader2.join()
    writer1.join()
    writer2.join()
    reader3.join()

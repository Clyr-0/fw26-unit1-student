# This is the implementation for the framing protocol, providing methods to send and recieve messages, by
# converting messages into JSON strings and then byte strings for transmission.

import socket
import struct
import json


def recv_exact(sock, n):
    # Recursive implementation could technically run into python's recursion limit with large payloads if
    # byte transmission happens in suitably small pieces. The implemenation stays for now as part of trying
    # to practice writing recursive functions in general.
    if n == 0:
        return b""

    chunk = sock.recv(n)

    if not chunk:
        raise ConnectionError("Connection closed")

    return chunk + recv_exact(sock, n-len(chunk))


def send_message(sock, message):
    # Takes the message, creates the JSON string, encodes it into a byte string, ships it.
    json_string = json.dumps(message)
    payload = json_string.encode("utf-8")

    header = struct.pack('!I', len(payload))

    sock.sendall(header + payload)


def recv_message(sock):
    # Recieves the message, confirms the payload length, recieves the payload, decodes the message, then returns it.
    header = recv_exact(sock, 4)
    length = struct.unpack("!I", header)[0]  # struct.unpack returns a tuple.
    payload = recv_exact(sock, length)

    json_string = payload.decode('utf-8')
    message = json.loads(json_string)

    return message

################################ Not needed for assignment deliverables #######################################


class LimitedSocket:
    # Test to see if the header is read correctly when recieving less than 4 bytes
    def __init__(self, sock, max_chunk):
        self.sock = sock
        self.max_chunk = max_chunk

    def recv(self, n):
        return self.sock.recv(min(n, self.max_chunk))


if __name__ == '__main__':
    print(f'Running test for protocol.py')

    sock1, sock2 = socket.socketpair()

    stress_tester = LimitedSocket(sock2, 2)

    message1 = {'operation': 'put', 'key': 'name', 'value': 'First Name'}
    message2 = {'operation': 'put', 'key': 'name', 'value': 'Last Name'}

    send_message(sock1, message1)
    send_message(sock1, message2)

    # This is to test that the function actually uses the payload header correctly, since both messages will sit
    # in the buffer.
    received1 = recv_message(stress_tester)
    received2 = recv_message(stress_tester)

    assert message1 == received1, "Mismatched 1"
    assert message2 == received2, "Mismatched 2"

    # Testing the recursions base case
    assert recv_exact(sock2, 0) == b''

    # Testing the closed connection logic by sending an incomplete header
    try:
        # Sends two bytes (half a header) then closes.
        sock1.sendall(b'\x00\x00')
        sock1.close()

        # Attempts to read the message header. Should throw a ConnectionError
        received = recv_exact(sock2, 4)

        assert False, 'ConnectionError not raised'

    except ConnectionError:
        print("Disconnection test passed")

    finally:
        sock2.close()

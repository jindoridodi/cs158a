import uuid
import threading
import socket


# Message Class
class Message:
    def __init__(self, uuid, flag=0):
        self.uuid = uuid
        self.flag = flag


# Node Class
class Node:
    def __init__(self, server_ip, server_port, neighbor_ip, neighbor_port):
        self.uuid = uuid.uuid4()
        self.flag = 0
        self.leader_id = None
        self.server_ip = server_ip
        self.server_port = server_port
        self.neighbor_ip = neighbor_ip
        self.neighbor_port = neighbor_port


# Server
def server(server_ip, server_port):
    # Create TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        # Allows port to be reused instantly
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind neighbor ip and port to socker, then listen
        server_sock.bind((server_ip, server_port))
        server_sock.listen(1)

        while True:
            conn, addr = server_sock.accept()

            with conn:
                pass


# Client
def client(uuid):
    # Open config.txt
    with open("config.txt", "r") as config:
        server_ip = config.readline()
        client_ip = config.readline()

    with open("logs.txt", "w") as log:
        log.writelines(f"UUID: {uuid}")

    # Create the TCP socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((server_ip, client_ip))

    # Create message and send
    message = Message(uuid)
    sock.sendall(message.uuid.encode())


# Main Function
def main():
    with open("config.txt", "r") as config:
        server_ip_and_port = config.readline()
        client_ip_and_port = config.readline()

    # Parse
    server_ip = server_ip_and_port.split(",")[0]
    server_port = server_ip_and_port.split(",")[1]

    # Start threads
    server_thread = threading.Thread(server(server_ip, server_port))
    client_thread = threading.Thread(client(uuid))


if __name__ == "__main__":
    main()

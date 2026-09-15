import uuid
import threading
import socket
import time

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

        # IP Information
        self.server_ip = server_ip
        self.server_port = server_port
        self.neighbor_ip = neighbor_ip
        self.neighbor_port = neighbor_port

        # Connection Information
        self.server_connection = None
        self.client_socket = None


# Server
def server(self, server_ip, server_port):
    # Create TCP socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        # Allows port to be reused instantly
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind server ip and server port to socker, then listen
        server_sock.bind((server_ip, server_port))
        server_sock.listen(1)

        # Print to terminal
        print(f"Server IP and Port:{self.server_ip}:{self.server_port}")

        # Connect to neighbor and print to terminal
        self.server_connection, addr = server_sock.accept()
        print(f"Connected to {addr}")


# Client
def client(self, neighbor_ip, neighbor_port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_sock:
        while True:
            try:
                client_sock.connect((self.neighbor_ip, self.neighbor_port))
                break
            except ConnectionRefusedError:
                time.sleep(1)


# Main Function
def main():
    with open("config.txt", "r") as config:
        server_ip_and_port = config.readline().strip()
        neighbor_ip_and_port = config.readline().strip()

    # Parse server ip and port
    server_ip, server_port = server_ip_and_port.split(",")

    # Parse neighbor ip and port
    neighbor_ip, neighbor_port = neighbor_ip_and_port.split(",")

    # Make node
    node = Node(server_ip, server_port, neighbor_ip, neighbor_port)

    # Log and print to terminal
    with open("log.txt", "w") as log:
        log.write(f"UUID: {node.uuid}\n")
    print(f"UUID: {node.uuid}\n")

    # Start threads
    server_thread = threading.Thread(target=server, args=(node,))
    client_thread = threading.Thread(target=client, args=(node,))


if __name__ == "__main__":
    main()

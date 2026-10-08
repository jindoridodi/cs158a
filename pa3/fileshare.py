import socket
import threading


# Node Class
class Node:
    def __init__(self, server_ip: str, server_port: int):
        # IP Information
        self.server_ip: str = server_ip
        self.server_port: int = server_port

        # Peers Information
        self.peers: dict[str, Peer] = {}

        # Connection Information
        self.server_socket: socket.socket | None = None


# Peer Class
class Peer:
    def __init__(self, peer_ip: str, peer_port: int, connection: socket.socket):
      self.peer_ip: str = peer_ip
      self.peer_port: int = peer_port
      self.connection: socket.socket  = connection


# Message Class
class Message:
    def __init__(self, message_type, data):
        self.message_type: str = message_type
        self.data: dict = data


# Helper function for logs
def write_log(log: str) -> None:
    # Print log to terminal
    print(log)

    # Write log to txt
    with open("log.txt", "a") as log_file:
        log_file.write(log + "\n")


def UDP_Broadcast():
    # Create UDP socket
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        pass


def send_message():
    pass


def receive_message():
    pass


def peer_request():
    pass


def main():
    # Create Thread for UDP Broadcast
    UDP_Thread = threading.Thread(target=UDP_Broadcast)

    # Create Thread for TCP
    
    # Start Threads

    pass


if __name__ == "__main__":
    main()

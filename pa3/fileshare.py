import socket
import threading
import time
import json
import struct


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
        self.connection: socket.socket = connection


# Helper function for logs
def write_log(log: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] {log}"

    print(entry)

    with open("log.txt", "a", encoding="utf-8") as log_file:
        log_file.write(entry + "\n")


def UDP_Broadcast():
    # Create UDP socket
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        pass


def send_message(connection: socket.socket, message: dict) -> None:
    # Convert JSON to UTF-8
    data_payload = json.dumps(message).encode("utf-8")

    # Make header to know where TCP message ends
    header = struct.pack("!I", len(data_payload))

    # Send message
    connection.sendall(data_payload + header)


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

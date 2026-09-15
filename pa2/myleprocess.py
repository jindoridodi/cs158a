import uuid
import threading
import socket
import time
import json


# Message Class
class Message:
    def __init__(self, uuid: uuid.UUID, flag: int = 0):
        self.uuid: uuid.UUID = uuid
        self.flag: int = flag


# Node Class
class Node:
    def __init__(
        self, server_ip: str, server_port: int, neighbor_ip: str, neighbor_port: int
    ):
        self.uuid: uuid.UUID = uuid.uuid4()
        self.flag: int = 0
        self.leader_id: uuid.UUID | None = None

        # IP Information
        self.server_ip: str = server_ip
        self.server_port: int = server_port
        self.neighbor_ip: str = neighbor_ip
        self.neighbor_port: int = neighbor_port

        # Connection Information
        self.server_connection: socket.socket | None = None
        self.client_socket: socket.socket | None = None


# Server
def server(node: Node) -> None:
    # Create TCP socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        # Allows port to be reused instantly
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind server ip and server port to socker, then listen
        server_sock.bind((node.server_ip, node.server_port))
        server_sock.listen(1)

        # Print to terminal
        print(f"Server is on. IP and Port:{node.server_ip}:{node.server_port}")

        # Accept connection from neighbor and print to terminal
        node.server_connection, addr = server_sock.accept()
        print(f"Accepted connection from {addr}")


# Client
def client(node: Node) -> None:
    # Connect to neighbor
    while True:
        try:
            node.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            node.client_socket.connect((node.neighbor_ip, node.neighbor_port))
            break
        except OSError:
            # Sleep if cannot connect
            time.sleep(1)

    # Print status to terminal
    print(f"Connected to neighbor {node.neighbor_ip}:{node.neighbor_port}")


# Helper function to convert to JSON
def convert_to_json(message: Message) -> str:
    return json.dumps({"uuid": str(message.uuid), "flag": message.flag})


# Helper function to convert back to Message
def convert_to_message(data: str) -> Message:
    msg = json.loads(data)
    return Message(uuid.UUID(msg["uuid"]), msg["flag"])


# Send Message
def send_message(node: Node, message: Message) -> None:
    # Ensure client socket is not None
    if node.client_socket is None:
        raise ConnectionError("No Client Connection")

    # Send encoded data
    msg_data = convert_to_json(message)
    node.client_socket.sendall(msg_data.encode())

    print(f"Sent: uuid={message.uuid}, flag={message.flag}")


# Receive Message
def receive_message(node: Node) -> None:
    # Ensure server connection is not None
    if node.server_connection is None:
        raise ConnectionError("No Server Connection")

    buf = ""
    while True:
        # Get data from prev node
        data = node.server_connection.recv(1024)

        # No connection
        if not data:
            break

        # Add to buffer
        buf += data.decode()

        # } is the end of message
        while "}" in buf:
            end_index = buf.index("}")

            # Get one full JSON msg
            msg_data = buf[: end_index + 1]  # + 1 because end index is exclusive

            # Remove msg from buffer
            buf = buf[end_index + 1 :]

            # Convert JSON str to Message
            message = convert_to_message(msg_data)

            print(f"Received: uuid={message.uuid}, flag={message.flag}")


# Main Function
def main():
    with open("config.txt", "r") as config:
        server_ip_and_port = config.readline().strip()
        neighbor_ip_and_port = config.readline().strip()

    # Parse server ip and port
    server_ip, server_port = server_ip_and_port.split(",")

    # Parse neighbor ip and port
    neighbor_ip, neighbor_port = neighbor_ip_and_port.split(",")

    # Convert ports to int
    server_port = int(server_port)
    neighbor_port = int(neighbor_port)

    # Make node
    node = Node(server_ip, server_port, neighbor_ip, neighbor_port)

    # Log and print to terminal
    with open("log.txt", "w") as log:
        log.write(f"UUID: {node.uuid}\n")
    print(f"UUID: {node.uuid}\n")

    # Create threads
    server_thread = threading.Thread(target=server, args=(node,))
    client_thread = threading.Thread(target=client, args=(node,))

    # Start Threads
    server_thread.start()
    client_thread.start()

    # Join Threads
    server_thread.join()
    client_thread.join()

    # At this point, both sides are connected
    print("Established both connections.")

    # Send Message
    message = Message(node.uuid)
    send_message(node, message)

    # Receive Message
    receive_message(node)


if __name__ == "__main__":
    main()

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

        # Bind server ip and server port to socket, then listen
        server_sock.bind((node.server_ip, node.server_port))
        server_sock.listen(1)

        # Print to terminal
        write_log(f"Server is on. IP and Port:{node.server_ip}:{node.server_port}")

        # Accept connection from neighbor and print to terminal
        node.server_connection, addr = server_sock.accept()
        write_log(f"Accepted connection from {addr}")


# Client
def client(node: Node) -> None:
    # Connect to neighbor
    while True:
        try:
            node.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            node.client_socket.connect((node.neighbor_ip, node.neighbor_port))
            break
        except OSError:
            # Close any exising socket
            if node.client_socket is not None:
                node.client_socket.close()
            node.client_socket = None
            # Sleep if cannot connect
            time.sleep(1)

    # Print status to terminal
    write_log(f"Connected to neighbor {node.neighbor_ip}:{node.neighbor_port}")


# Helper function to convert to JSON
def convert_to_json(message: Message) -> str:
    return json.dumps({"uuid": str(message.uuid), "flag": message.flag})


# Helper function to convert back to Message
def convert_to_message(data: str) -> Message:
    msg = json.loads(data)
    return Message(uuid.UUID(msg["uuid"]), msg["flag"])


# Helper function for logs
def write_log(log: str) -> None:
    # Print log to terminal
    print(log)

    # Write log to txt
    with open("log.txt", "a") as log_file:
        log_file.write(log + "\n")


# Send Message
def send_message(node: Node, message: Message) -> None:
    # Ensure client socket is not None
    if node.client_socket is None:
        raise ConnectionError("No Client Connection")

    # Send encoded data
    msg_data = convert_to_json(message)
    node.client_socket.sendall(msg_data.encode())

    write_log(f"Sent: uuid={message.uuid}, flag={message.flag}")


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

            # Process recieved message
            leader_found = process_message(node, message)

            # Exit once leader has been found
            if leader_found:
                return


# Function for determining whether to forward or ignore UUID and finally determining a leader
# Returns boolean whether leader has been found or not
def process_message(node: Node, message: Message) -> bool:
    # Check if leader has not been found yet
    if message.flag == 0:
        # Received UUID > Process UUID: Forward message
        if message.uuid > node.uuid:
            write_log(
                f"Received: uuid={message.uuid}, flag={message.flag}, greater, {node.flag}"
            )
            # Forward message
            send_message(node, message)
            return False

        # Received UUID < Process UUID: Ignore message
        elif message.uuid < node.uuid:
            write_log(
                f"Received: uuid={message.uuid}, flag={message.flag}, less, {node.flag}"
            )
            write_log("Received message was ignored.")
            return False

        # Received UUID == Process UUID: This process is the leader
        else:
            write_log(
                f"Received: uuid={message.uuid}, flag={message.flag}, same, {node.flag}"
            )

            # Update flag and store leader
            node.flag = 1
            node.leader_id = node.uuid
            write_log(f"Leader is decided to {node.leader_id}.")

            # Tell other nodes the leader's UUID
            leader_msg = Message(node.leader_id, 1)
            send_message(node, leader_msg)

            return False

    # Leader has already been found
    else:
        # Leader Process:
        # Message has went around the ring, so do not forward message. Only Log.
        if message.uuid == node.uuid:
            write_log(
                f"Received: uuid={message.uuid}, flag={message.flag}, same, {node.flag}, leader_id={node.leader_id}"
            )
            write_log(f"Leader is {node.leader_id}")
            return True

        # Non-Leader Process:
        # Update flag and store leader
        node.flag = 1
        node.leader_id = message.uuid

        # Compare UUID and log
        if message.uuid > node.uuid:
            comparison = "greater"
        elif message.uuid < node.uuid:
            comparison = "less"
        else:
            comparison = "same"
        write_log(
            f"Received: uuid={message.uuid}, flag={message.flag}, {comparison}, {node.flag}, leader_id={node.leader_id}"
        )

        # Forward leader message and terminate
        send_message(node, message)
        write_log(f"Leader is {node.leader_id}")
        return True


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
    with open("log.txt", "w") as log_file:
        log_file.write(f"UUID: {node.uuid}\n")
    print(f"UUID: {node.uuid}")

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
    write_log("Established both connections.")

    # Send Message
    message = Message(node.uuid)
    send_message(node, message)

    # Receive Message
    receive_message(node)


if __name__ == "__main__":
    main()

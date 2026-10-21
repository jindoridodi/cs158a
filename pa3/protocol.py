from __future__ import annotations

import socket
import threading
import time
import json
import struct

UDP_PORT = 54321
HEADER_SIZE = 4
LOG_FILE = "log.txt"
_log_lock = threading.Lock()


def write_log(text: str) -> None:
    """Print and append a timestamped line to LOG_FILE under _log_lock."""
    # Add a timestamp to the log message
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] {text}"

    # Lock so threads do not write over each other
    with _log_lock:
        # Print the log to the terminal
        print(entry)

        # Append the log to the log file
        with open(LOG_FILE, "a", encoding="utf-8") as log_file:
            log_file.write(entry + "\n")


def log_message(direction: str, addr: tuple[str, int], msg_type: str) -> None:
    """Log a SENT or RECEIVED message type and its remote IP and port."""
    # Separate the peer's IP address and port
    ip, port = addr

    # Write the message direction, peer, and type to the log
    write_log(f"{direction} peer={ip}:{port} type={msg_type}")


def log_peer_change(event: str, node_id: str, addr: tuple[str, int]) -> None:
    """Log an ADDED or REMOVED peer with its node ID, IP, and port."""
    # Separate the peer's IP address and port
    ip, port = addr

    # Write the peer table change to the log
    write_log(f"PEER {event} node_id={node_id} addr={ip}:{port}")


def make_peer_request(node_id: str) -> dict:
    """Build a PEER_REQUEST message containing the sender's node ID."""
    raise NotImplementedError("TODO: Person 2")


def make_peer_ack(node_id: str, tcp_port: int) -> dict:
    """Build a PEER_ACK message advertising the sender's TCP port."""
    raise NotImplementedError("TODO: Person 2")


def make_file_list(node_id: str, files: list[str]) -> dict:
    """Build a FILE_LIST message with the sender's shared filenames."""
    raise NotImplementedError("TODO: Person 2")


def make_data_request(node_id: str, filename: str) -> dict:
    """Build a DATA_REQUEST message for one filename."""
    raise NotImplementedError("TODO: Person 2")


def make_file_data(node_id: str, filename: str, data: bytes | None) -> dict:
    """Build FILE_DATA with base64 content or a NOT_FOUND response for None."""
    raise NotImplementedError("TODO: Person 2")


def encode_udp(message: dict) -> bytes:
    """Encode one message as a UTF-8 JSON UDP datagram."""
    raise NotImplementedError("TODO: Person 2")


def decode_udp(data: bytes) -> dict | None:
    """Decode UTF-8 JSON, or return None for invalid data or missing keys."""
    raise NotImplementedError("TODO: Person 2")


def send_message(peer, message: dict) -> None:
    """Send a framed and logged message through a peer's locked socket."""
    # Convert JSON to UTF-8
    data_payload = json.dumps(message).encode("utf-8")

    # Make header to know where TCP message ends
    header = struct.pack("!I", len(data_payload))

    # Send message
    with peer.send_lock:
        peer.connection.sendall(header + data_payload)
    log_message("SENT", (peer.ip, peer.tcp_port), message["type"])


def receive_message(conn: socket.socket, addr: tuple[str, int]) -> dict | None:
    """Read exact framing and JSON; log it or return None on disconnect."""
    try:
        # Read the complete 4-byte header
        header = b""
        while len(header) < HEADER_SIZE:
            data = conn.recv(HEADER_SIZE - len(header))
            if not data:
                return None
            header += data

        # Convert the header to the payload size
        payload_size = struct.unpack("!I", header)[0]

        # Read the complete JSON payload
        data_payload = b""
        while len(data_payload) < payload_size:
            data = conn.recv(payload_size - len(data_payload))
            if not data:
                return None
            data_payload += data

        # Convert the UTF-8 JSON payload to a dictionary
        message = json.loads(data_payload.decode("utf-8"))
        if not isinstance(message, dict) or "type" not in message:
            return None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, struct.error):
        return None

    # Log and return the received message
    log_message("RECEIVED", addr, message["type"])
    return message


def peer_loop(node, peer) -> None:
    """Dispatch peer messages, then remove the peer and close its socket."""
    raise NotImplementedError("TODO: Person 2")

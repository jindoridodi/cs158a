"""Person 2: message formatting, socket framing, and logging interfaces."""

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
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] {text}"

    print(entry)

    with open(LOG_FILE, "a", encoding="utf-8") as log_file:
        log_file.write(entry + "\n")


def log_message(direction: str, addr: tuple[str, int], msg_type: str) -> None:
    """Log a SENT or RECEIVED message type and its remote IP and port."""
    raise NotImplementedError("TODO: Person 2")


def log_peer_change(event: str, node_id: str, addr: tuple[str, int]) -> None:
    """Log an ADDED or REMOVED peer with its node ID, IP, and port."""
    raise NotImplementedError("TODO: Person 2")


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
    raise NotImplementedError("TODO: Person 2")


def peer_loop(node, peer) -> None:
    """Dispatch peer messages, then remove the peer and close its socket."""
    raise NotImplementedError("TODO: Person 2")

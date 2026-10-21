"""Person 2: message formatting, socket framing, and logging interfaces."""

from __future__ import annotations

import socket
import threading


UDP_PORT = 54321
HEADER_SIZE = 4
LOG_FILE = "log.txt"
_log_lock = threading.Lock()


def write_log(text: str) -> None:
    """Print and append a timestamped line to LOG_FILE under _log_lock."""
    raise NotImplementedError("TODO: Person 2")


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


def recv_exact(conn: socket.socket, n: int) -> bytes | None:
    """Read exactly n bytes, or return None if the peer closes the socket."""
    raise NotImplementedError("TODO: Person 2")


def send_on_socket(
    conn: socket.socket,
    lock: threading.Lock,
    addr: tuple[str, int],
    message: dict,
) -> None:
    """Lock, send a four-byte big-endian length and JSON, then log it."""
    raise NotImplementedError("TODO: Person 2")


def send_message(peer, message: dict) -> None:
    """Send a message using the peer's socket, send lock, and address."""
    raise NotImplementedError("TODO: Person 2")


def receive_message(conn: socket.socket, addr: tuple[str, int]) -> dict | None:
    """Read and log framed JSON; return None on errors or disconnect."""
    raise NotImplementedError("TODO: Person 2")


def peer_loop(node, peer) -> None:
    """Dispatch peer messages, then remove the peer and close its socket."""
    raise NotImplementedError("TODO: Person 2")

from __future__ import annotations

import base64
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
    # Identify the broadcast and the node that sent it
    return {
        "type": "PEER_REQUEST",
        "node_id": node_id,
    }


def make_peer_ack(node_id: str, tcp_port: int) -> dict:
    """Build a PEER_ACK message advertising the sender's TCP port."""
    # Identify the response and advertise the node's TCP port
    return {
        "type": "PEER_ACK",
        "node_id": node_id,
        "tcp_port": tcp_port,
    }


def make_file_list(node_id: str, files: list[str]) -> dict:
    """Build a FILE_LIST message with the sender's shared filenames."""
    # Copy the filenames so later list changes do not change the message
    return {
        "type": "FILE_LIST",
        "node_id": node_id,
        "files": list(files),
    }


def make_data_request(node_id: str, filename: str) -> dict:
    """Build a DATA_REQUEST message for one filename."""
    # Identify the request and the desired filename
    return {
        "type": "DATA_REQUEST",
        "node_id": node_id,
        "filename": filename,
    }


def make_file_data(node_id: str, filename: str, data: bytes | None) -> dict:
    """Build FILE_DATA with base64 content or a NOT_FOUND response for None."""
    # Create the fields shared by successful and failed responses
    message: dict[str, object] = {
        "type": "FILE_DATA",
        "node_id": node_id,
        "filename": filename,
    }

    # Report a file that no longer exists
    if data is None:
        message["status"] = "NOT_FOUND"
        message["content"] = None
        return message

    # Encode the file bytes as text so they can be placed inside JSON
    message["status"] = "OK"
    message["content"] = base64.b64encode(data).decode("ascii")
    return message


def encode_udp(message: dict) -> bytes:
    """Encode one message as a UTF-8 JSON UDP datagram."""
    # Convert the message dictionary to compact JSON
    data_payload = json.dumps(message, separators=(",", ":"))

    # Convert the JSON text to bytes for the UDP socket
    return data_payload.encode("utf-8")


def decode_udp(data: bytes) -> dict | None:
    """Decode UTF-8 JSON, or return None for invalid data or missing keys."""
    try:
        # Convert the UDP bytes to a JSON value
        message = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None

    # Require every discovery message to identify its type and sender
    if not isinstance(message, dict):
        return None
    if message.get("type") not in {"PEER_REQUEST", "PEER_ACK"}:
        return None
    if not isinstance(message.get("node_id"), str):
        return None

    # Require an ACK to advertise a valid TCP port
    if message["type"] == "PEER_ACK":
        tcp_port = message.get("tcp_port")
        if (
            not isinstance(tcp_port, int)
            or isinstance(tcp_port, bool)
            or not 1 <= tcp_port <= 65535
        ):
            return None

    return message


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
        if not isinstance(message, dict):
            return None
        if not isinstance(message.get("type"), str):
            return None
        if not isinstance(message.get("node_id"), str):
            return None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, struct.error):
        return None

    # Log and return the received message
    log_message("RECEIVED", addr, message["type"])
    return message


def peer_loop(node, peer) -> None:
    """Dispatch peer messages, then remove the peer and close its socket."""
    addr = (peer.ip, peer.tcp_port)

    try:
        while True:
            # Wait for the peer's next complete TCP message
            message = receive_message(peer.connection, addr)
            if message is None:
                break

            # Ignore messages that claim to come from a different peer
            if message.get("node_id") != peer.node_id:
                write_log(f"Ignored message with wrong node_id from {peer.ip}")
                continue

            message_type = message["type"]

            # Replace the peer's advertised file list
            if message_type == "FILE_LIST":
                files = message.get("files")
                if isinstance(files, list) and all(
                    isinstance(filename, str) for filename in files
                ):
                    node.peers.set_files(peer.node_id, files)

            # Ask the node to serve the requested file
            elif message_type == "DATA_REQUEST":
                if isinstance(message.get("filename"), str):
                    node.handle_data_request(peer, message)

            # Ask the node to process the returned file or error
            elif message_type == "FILE_DATA":
                status = message.get("status")
                valid_content = (
                    status == "NOT_FOUND" and message.get("content") is None
                ) or (
                    status == "OK"
                    and isinstance(message.get("content"), str)
                )
                if isinstance(message.get("filename"), str) and valid_content:
                    node.handle_file_data(peer, message)

            # Ignore message types that are not used over TCP
            else:
                write_log(
                    f"Ignored unsupported message type {message_type} "
                    f"from {peer.ip}:{peer.tcp_port}"
                )
    finally:
        # Remove only this connection from the peer table
        if node.peers.remove(peer.node_id, peer.connection):
            log_peer_change("REMOVED", peer.node_id, addr)

        # Close the disconnected peer's socket
        peer.connection.close()

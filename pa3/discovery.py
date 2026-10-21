from __future__ import annotations

import socket
import threading
import time

import protocol


class Peer:
    """Represent one established TCP connection to a remote node."""

    def __init__(
        self,
        node_id: str,
        ip: str,
        tcp_port: int,
        connection: socket.socket,
    ) -> None:
        """Store peer identity, connection, advertised files, and send lock."""
        self.node_id = node_id
        self.ip = ip
        self.tcp_port = tcp_port
        self.connection = connection
        self.files: list[str] = []
        self.send_lock = threading.Lock()


class PeerTable:
    """Keep peer and pending-connection state safe across agent threads."""

    def __init__(self) -> None:
        """Create an empty table guarded by one lock."""
        self._lock = threading.Lock()
        self._peers: dict[str, Peer] = {}
        self._connecting: set[str] = set()

    def register(
        self, peer: Peer, new_is_preferred: bool
    ) -> tuple[bool, Peer | None]:
        """Add a peer or atomically replace its current connection."""
        with self._lock:
            old_peer = self._peers.get(peer.node_id)
            if old_peer is None:
                self._peers[peer.node_id] = peer
                return True, None
            if new_is_preferred:
                self._peers[peer.node_id] = peer
                return True, old_peer
            return False, None

    def remove(self, node_id: str, conn: socket.socket) -> bool:
        """Remove a peer only when conn is its currently stored socket."""
        with self._lock:
            peer = self._peers.get(node_id)
            if peer is None or peer.connection is not conn:
                return False
            del self._peers[node_id]
            return True

    def get(self, node_id: str) -> Peer | None:
        """Return a peer by node ID, or None if it is unknown."""
        with self._lock:
            return self._peers.get(node_id)

    def has(self, node_id: str) -> bool:
        """Report whether a node ID has an established peer connection."""
        with self._lock:
            return node_id in self._peers

    def all(self) -> list[Peer]:
        """Return a snapshot of all currently registered peers."""
        with self._lock:
            return list(self._peers.values())

    def set_files(self, node_id: str, files: list[str]) -> None:
        """Replace a known peer's advertised files with a private copy."""
        with self._lock:
            peer = self._peers.get(node_id)
            if peer is not None:
                peer.files = list(files)

    def files_by_peer(self) -> dict[str, list[str]]:
        """Return an independent snapshot of each peer's advertised files."""
        with self._lock:
            return {
                node_id: list(peer.files)
                for node_id, peer in self._peers.items()
            }

    def owners_of(self, filename: str) -> list[Peer]:
        """Return peers that advertise the given filename."""
        with self._lock:
            return [
                peer for peer in self._peers.values() if filename in peer.files
            ]

    def start_connecting(self, node_id: str) -> bool:
        """Reserve a node ID unless it is connected or being connected."""
        with self._lock:
            if node_id in self._peers or node_id in self._connecting:
                return False
            self._connecting.add(node_id)
            return True

    def done_connecting(self, node_id: str) -> None:
        """Clear a node ID's pending outgoing-connection reservation."""
        with self._lock:
            self._connecting.discard(node_id)


def _register_peer(node, peer: Peer, i_am_client: bool) -> bool:
    """Keep the connection dialed by the lexically lower node ID."""
    client_id = node.node_id if i_am_client else peer.node_id
    server_id = peer.node_id if i_am_client else node.node_id
    kept, old = node.peers.register(
        peer, new_is_preferred=client_id < server_id
    )
    if kept:
        protocol.log_peer_change(
            "ADDED", peer.node_id, (peer.ip, peer.tcp_port)
        )
        if old is not None:
            protocol.write_log("duplicate connection replaced")
            old.connection.close()
    else:
        protocol.write_log("duplicate connection dropped")
        peer.connection.close()
    return kept


def start(node) -> None:
    """Start the TCP server and three daemon discovery threads."""
    start_tcp_server(node)
    for target in (accept_loop, udp_listener, broadcast_peer_request):
        threading.Thread(target=target, args=(node,), daemon=True).start()


def start_tcp_server(node) -> None:
    """Bind and listen on the node's TCP port, storing the server socket."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("", node.tcp_port))
        server.listen()
    except OSError:
        server.close()
        raise
    node.server_socket = server
    protocol.write_log(
        f"Listening on TCP port {node.tcp_port}, node_id {node.node_id}"
    )


def accept_loop(node) -> None:
    """Accept TCP clients and hand each handshake to a daemon thread."""
    while True:
        try:
            conn, addr = node.server_socket.accept()
        except OSError:
            return
        threading.Thread(
            target=handle_incoming,
            args=(node, conn, addr),
            daemon=True,
        ).start()


def handle_incoming(node, conn, addr) -> None:
    """Validate the client's FILE_LIST before adding it to the peer table."""
    try:
        conn.settimeout(10)
        msg = protocol.receive_message(conn, addr)
        conn.settimeout(None)
    except OSError:
        conn.close()
        return

    peer_id = msg.get("node_id") if msg is not None else None
    files = msg.get("files") if msg is not None else None
    if (
        msg is None
        or msg.get("type") != "FILE_LIST"
        or not isinstance(peer_id, str)
        or not peer_id
        or peer_id == node.node_id
        or not isinstance(files, list)
        or not all(isinstance(filename, str) for filename in files)
    ):
        conn.close()
        return

    peer = Peer(peer_id, addr[0], addr[1], conn)
    if not _register_peer(node, peer, i_am_client=False):
        return

    node.peers.set_files(peer.node_id, files)
    try:
        protocol.send_message(
            peer,
            protocol.make_file_list(node.node_id, node.shared_files()),
        )
    except OSError:
        if node.peers.remove(peer.node_id, conn):
            protocol.log_peer_change(
                "REMOVED", peer.node_id, (peer.ip, peer.tcp_port)
            )
        conn.close()
        return
    protocol.peer_loop(node, peer)


def connect_to_peer(node, ip: str, port: int, peer_node_id: str) -> None:
    """Connect after an ACK, send local files, and start a receive loop."""
    if not node.peers.start_connecting(peer_node_id):
        return

    conn = None
    registered = False
    try:
        conn = socket.create_connection((ip, port), timeout=5)
        conn.settimeout(None)
        peer = Peer(peer_node_id, ip, port, conn)
        if not _register_peer(node, peer, i_am_client=True):
            return
        registered = True
        protocol.send_message(
            peer,
            protocol.make_file_list(node.node_id, node.shared_files()),
        )
        threading.Thread(
            target=protocol.peer_loop,
            args=(node, peer),
            daemon=True,
        ).start()
    except OSError as exc:
        protocol.write_log(f"Failed to connect to {ip}:{port}: {exc}")
        if registered and node.peers.remove(peer_node_id, conn):
            protocol.log_peer_change("REMOVED", peer_node_id, (ip, port))
        if conn is not None:
            conn.close()
    finally:
        node.peers.done_connecting(peer_node_id)


def udp_listener(node) -> None:
    """Answer valid PEER_REQUEST broadcasts on the shared discovery port."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(("", protocol.UDP_PORT))
        while True:
            try:
                data, addr = sock.recvfrom(65535)
            except OSError:
                return
            try:
                msg = protocol.decode_udp(data)
                if (
                    msg is None
                    or msg.get("node_id") == node.node_id
                    or msg.get("type") != "PEER_REQUEST"
                ):
                    continue
                protocol.log_message("RECEIVED", addr, "PEER_REQUEST")
                ack = protocol.make_peer_ack(node.node_id, node.tcp_port)
                sock.sendto(protocol.encode_udp(ack), addr)
                protocol.log_message("SENT", addr, "PEER_ACK")
            except Exception as exc:
                protocol.write_log(f"Failed to handle discovery packet: {exc}")
    finally:
        sock.close()


def broadcast_peer_request(node) -> None:
    """Broadcast three requests and collect ACKs for five seconds total."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("", 0))
        sock.settimeout(1.0)
        first_send_at = time.monotonic()
        for attempt in range(3):
            if attempt:
                time.sleep(1)
            try:
                request = protocol.make_peer_request(node.node_id)
                sock.sendto(
                    protocol.encode_udp(request),
                    ("<broadcast>", protocol.UDP_PORT),
                )
                protocol.log_message(
                    "SENT", ("<broadcast>", protocol.UDP_PORT), "PEER_REQUEST"
                )
            except OSError as exc:
                protocol.write_log(f"Failed to broadcast PEER_REQUEST: {exc}")

        deadline = first_send_at + 5
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            sock.settimeout(min(1.0, remaining))
            try:
                data, addr = sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError as exc:
                protocol.write_log(f"Failed to receive PEER_ACK: {exc}")
                break
            try:
                msg = protocol.decode_udp(data)
                if (
                    msg is None
                    or msg.get("node_id") == node.node_id
                    or msg.get("type") != "PEER_ACK"
                ):
                    continue
                protocol.log_message("RECEIVED", addr, "PEER_ACK")
                threading.Thread(
                    target=connect_to_peer,
                    args=(node, addr[0], msg["tcp_port"], msg["node_id"]),
                    daemon=True,
                ).start()
            except Exception as exc:
                protocol.write_log(f"Failed to handle PEER_ACK: {exc}")
    finally:
        sock.close()

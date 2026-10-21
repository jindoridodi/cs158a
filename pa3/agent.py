"""Person 3: peer-to-peer agent state, transfers, commands, and entry point."""

from __future__ import annotations

import os
import socket
import uuid

import discovery
import protocol


class Node:
    """Hold this node's identity, connections, and local file directories."""

    def __init__(self, tcp_port: int) -> None:
        """Create a node with a unique ID, peer table, and file directories."""
        self.node_id = uuid.uuid4().hex
        self.tcp_port = tcp_port
        self.peers = discovery.PeerTable()
        self.server_socket: socket.socket | None = None
        self.shared_dir = "shared"
        self.downloads_dir = "downloads"
        os.makedirs(self.shared_dir, exist_ok=True)
        os.makedirs(self.downloads_dir, exist_ok=True)

    def shared_files(self) -> list[str]:
        """Return sorted regular filenames directly inside shared_dir."""
        raise NotImplementedError("TODO: Person 3")

    def handle_data_request(self, peer, msg: dict) -> None:
        """Reject unsafe names; reply with shared bytes or NOT_FOUND."""
        raise NotImplementedError("TODO: Person 3")

    def handle_file_data(self, peer, msg: dict) -> None:
        """Report missing files; save OK data before sharing a copy."""
        raise NotImplementedError("TODO: Person 3")

    def request_file(self, node_id: str, filename: str) -> None:
        """Skip local files, report unknown peers, or send a DATA_REQUEST."""
        raise NotImplementedError("TODO: Person 3")

    def file_list_loop(self) -> None:
        """Advertise shared files every ten seconds, skipping socket errors."""
        raise NotImplementedError("TODO: Person 3")

    def command_loop(self) -> None:
        """List peers/files, request files by ID prefix, or quit cleanly."""
        raise NotImplementedError("TODO: Person 3")


def main() -> None:
    """Parse --port, start discovery and file-list thread, then read input."""
    raise NotImplementedError("TODO: Person 3")


if __name__ == "__main__":
    main()

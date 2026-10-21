from __future__ import annotations

import argparse
import base64
import binascii
from contextlib import contextmanager
import os
import shutil
import threading
import uuid
from pathlib import Path

import discovery
import protocol


FILE_LIST_INTERVAL = 10


class Node:
    """Hold this node's identity, connections, and local file directories."""

    def __init__(self, tcp_port: int) -> None:
        self.node_id = uuid.uuid4().hex
        self.tcp_port = tcp_port
        self.peers = discovery.PeerTable()
        self.server_socket = None
        self.shared_dir = Path("shared")
        self.downloads_dir = Path("downloads")
        self.shared_dir.mkdir(parents=True, exist_ok=True)
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        self._stop_event = threading.Event()
        self._files_lock = threading.Lock()
        self._download_locks_guard = threading.Lock()
        self._download_locks: dict[str, tuple[threading.Lock, int]] = {}

    @contextmanager
    def _download_name_lock(self, filename: str):
        """Serialize writes for one filename without blocking other transfers."""
        with self._download_locks_guard:
            lock, users = self._download_locks.get(
                filename, (threading.Lock(), 0)
            )
            self._download_locks[filename] = (lock, users + 1)

        lock.acquire()
        try:
            yield
        finally:
            lock.release()
            with self._download_locks_guard:
                lock, users = self._download_locks[filename]
                if users == 1:
                    del self._download_locks[filename]
                else:
                    self._download_locks[filename] = (lock, users - 1)

    def shared_files(self) -> list[str]:
        """Return sorted regular filenames directly inside shared_dir."""
        try:
            with os.scandir(self.shared_dir) as entries:
                return sorted(
                    entry.name for entry in entries if entry.is_file(follow_symlinks=False)
                )
        except OSError as exc:
            protocol.write_log(f"Could not list shared directory: {exc}")
            return []

    @staticmethod
    def _safe_filename(value: object) -> str | None:
        """Accept one basename only; never allow a peer to select a path."""
        if not isinstance(value, str) or not value or ".." in value:
            return None
        if "\x00" in value or Path(value).name != value or "/" in value or "\\" in value:
            return None
        return value

    def handle_data_request(self, peer, msg: dict) -> None:
        """Reject unsafe names; reply with shared bytes or NOT_FOUND."""
        requested = msg.get("filename")
        filename = self._safe_filename(requested)
        content = None
        if filename is not None:
            try:
                path = self.shared_dir / filename
                if path.is_file() and not path.is_symlink():
                    content = path.read_bytes()
            except OSError as exc:
                protocol.write_log(f"Could not read requested file {filename}: {exc}")

        response_name = filename if filename is not None else str(requested or "")
        try:
            response = protocol.make_file_data(self.node_id, response_name, content)
            protocol.send_message(peer, response)
        except OSError as exc:
            protocol.write_log(
                f"Could not serve {response_name} to {peer.node_id[:8]}: {exc}"
            )

    def handle_file_data(self, peer, msg: dict) -> None:
        """Report missing files; save OK data before sharing a copy."""
        filename = self._safe_filename(msg.get("filename"))
        if filename is None:
            protocol.write_log(f"DOWNLOAD rejected unsafe filename from {peer.node_id[:8]}")
            return

        status = msg.get("status")
        if status == "NOT_FOUND":
            detail = f"DOWNLOAD {filename} from {peer.node_id[:8]}: NOT_FOUND"
            protocol.write_log(detail)
            print(f"{peer.node_id[:8]} no longer has {filename}.")
            return
        if status != "OK" or not isinstance(msg.get("content"), str):
            detail = f"DOWNLOAD {filename} from {peer.node_id[:8]}: invalid response"
            protocol.write_log(detail)
            print(f"Invalid file response for {filename} from {peer.node_id[:8]}.")
            return

        try:
            data = base64.b64decode(msg["content"], validate=True)
        except (ValueError, binascii.Error):
            protocol.write_log(
                f"DOWNLOAD {filename} from {peer.node_id[:8]}: invalid base64 data"
            )
            print(f"Invalid base64 data for {filename} from {peer.node_id[:8]}.")
            return

        downloads_path = self.downloads_dir / filename
        shared_path = self.shared_dir / filename
        staged_path = self.shared_dir / f".{filename}.{uuid.uuid4().hex}.part"
        try:
            # Two peers can answer requests for the same name concurrently.
            # Serialize the complete downloads/ and shared/ update so their
            # bytes cannot overwrite each other midway through a write.
            with self._download_name_lock(filename):
                with self._files_lock:
                    if shared_path.exists():
                        protocol.write_log(
                            f"DOWNLOAD {filename} from {peer.node_id[:8]}: skipped, already shared"
                        )
                        print(f"Already have {filename} in shared/; keeping that copy.")
                        return
                    # Write the complete transfer outside shared/ first.
                    downloads_path.write_bytes(data)
                    # Stage the copy in shared/, then atomically publish it.
                    shutil.copy2(downloads_path, staged_path)
                    try:
                        os.link(staged_path, shared_path)
                    except FileExistsError:
                        protocol.write_log(
                            f"DOWNLOAD {filename} from {peer.node_id[:8]}: skipped, already shared"
                        )
                        print(f"Already have {filename} in shared/; keeping that copy.")
                        return
            protocol.write_log(f"DOWNLOAD {filename} from {peer.node_id[:8]}: complete")
            print(f"Downloaded {filename} from {peer.node_id[:8]}.")
        except OSError as exc:
            protocol.write_log(
                f"DOWNLOAD {filename} from {peer.node_id[:8]}: failed to save: {exc}"
            )
            print(f"Could not save {filename}: {exc}")
        finally:
            try:
                staged_path.unlink()
            except FileNotFoundError:
                pass
            except OSError as exc:
                protocol.write_log(f"Could not remove staging file {staged_path}: {exc}")

    def request_file(self, node_id: str, filename: str) -> None:
        """Skip local files, report unknown peers, or send a DATA_REQUEST."""
        safe_name = self._safe_filename(filename)
        if safe_name is None:
            print("Use a filename without directory components.")
            return

        with self._files_lock:
            if (self.shared_dir / safe_name).exists():
                print(f"Already have {safe_name} in shared/; keeping that copy.")
                return

        peer = self.peers.get(node_id)
        if peer is None:
            print(f"No connected peer matches '{node_id[:8]}'.")
            return
        if safe_name not in peer.files:
            print(f"{peer.node_id[:8]} is not advertising {safe_name}.")
            return

        try:
            request = protocol.make_data_request(self.node_id, safe_name)
            protocol.send_message(peer, request)
            print(f"Requested {safe_name} from {peer.node_id[:8]}.")
        except OSError as exc:
            print(f"Could not request {safe_name} from {peer.node_id[:8]}: {exc}")

    def file_list_loop(self) -> None:
        """Advertise shared files every ten seconds, skipping socket errors."""
        while not self._stop_event.wait(FILE_LIST_INTERVAL):
            message = protocol.make_file_list(self.node_id, self.shared_files())
            for peer in self.peers.all():
                try:
                    protocol.send_message(peer, message)
                except OSError as exc:
                    protocol.write_log(
                        f"Could not send FILE_LIST to {peer.node_id[:8]}: {exc}"
                    )

    def _peer_matches(self, prefix: str):
        peers = self.peers.all()
        matches = [peer for peer in peers if peer.node_id.startswith(prefix)]
        if len(matches) == 1:
            return matches[0]
        if not matches:
            print(f"No connected peer matches '{prefix}'.")
        else:
            ids = ", ".join(peer.node_id[:8] for peer in matches)
            print(
                f"Peer ID prefix '{prefix}' is ambiguous ({ids}); "
                "use more characters."
            )
        return None

    def _print_peers(self) -> None:
        peers = self.peers.all()
        if not peers:
            print("No connected peers.")
            return
        for peer in peers:
            print(f"{peer.node_id[:8]}\t{peer.ip}:{peer.tcp_port}")

    def _print_files(self) -> None:
        peers = self.peers.all()
        if not peers:
            print("No connected peers.")
            return
        for peer in peers:
            print(f"{peer.node_id[:8]} ({peer.ip}:{peer.tcp_port}):")
            if peer.files:
                for filename in sorted(peer.files):
                    print(f"  {filename}")
            else:
                print("  (no files advertised)")

    def _shutdown(self) -> None:
        self._stop_event.set()
        if self.server_socket is not None:
            try:
                self.server_socket.close()
            except OSError:
                pass
        for peer in self.peers.all():
            try:
                peer.connection.shutdown(2)
            except OSError:
                pass
            try:
                peer.connection.close()
            except OSError:
                pass

    def command_loop(self) -> None:
        """List peers/files, request files by ID prefix, or quit cleanly."""
        print("Commands: peers | files | get <node_id_prefix> <filename> | quit")
        while not self._stop_event.is_set():
            try:
                line = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                line = "quit"
            if not line:
                continue

            parts = line.split(maxsplit=2)
            command = parts[0].lower()
            if command == "peers" and len(parts) == 1:
                self._print_peers()
            elif command == "files" and len(parts) == 1:
                self._print_files()
            elif command == "get" and len(parts) == 3:
                peer = self._peer_matches(parts[1])
                if peer is not None:
                    self.request_file(peer.node_id, parts[2])
            elif command == "quit" and len(parts) == 1:
                self._shutdown()
                return
            else:
                print("Usage: peers | files | get <node_id_prefix> <filename> | quit")


def main() -> None:
    """Parse --port, start discovery and file-list thread, then read input."""
    parser = argparse.ArgumentParser(description="P2P file sharing agent")
    parser.add_argument("--port", type=int, required=True, help="TCP listening port")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")

    node = Node(args.port)
    try:
        discovery.start(node)
    except OSError as exc:
        parser.error(f"could not start discovery/TCP server: {exc}")
    threading.Thread(target=node.file_list_loop, daemon=True).start()
    try:
        node.command_loop()
    finally:
        node._shutdown()


if __name__ == "__main__":
    main()

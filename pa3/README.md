# PA3 - P2P File Sharing

## Run

From `pa3/`, run each node with a different TCP port:

```text
python agent.py --port 6001
```

Commands:

- `peers`: Show all connected peers.
- `files`: Show the files advertised by each peer.
- `get <node_id_prefix> <filename>`: Request a file from a peer.
- `quit`: Close the node and exit.

## Message format

Messages are UTF-8 JSON objects. Every message contains `type` and the sender's
`node_id`.

| Type | Transport | Additional fields |
| --- | --- | --- |
| `PEER_REQUEST` | UDP port 54321 | None |
| `PEER_ACK` | UDP | `tcp_port` |
| `FILE_LIST` | TCP | `files` (list of filenames) |
| `DATA_REQUEST` | TCP | `filename` |
| `FILE_DATA` | TCP | `filename`, `status`, `content` |

UDP messages are plain JSON datagrams. TCP messages are a four-byte unsigned
big-endian JSON byte length (`struct.pack("!I", n)`) followed by exactly `n`
JSON bytes.

For `FILE_DATA`, `status` is `"OK"` and `content` is base64 text, or `status`
is `"NOT_FOUND"` and `content` is `null`. Base64 was chosen because it safely
stores binary data in JSON and the demo files are small.

```json
{"type": "PEER_REQUEST", "node_id": "3f9a1c"}
{"type": "PEER_ACK", "node_id": "3f9a1c", "tcp_port": 6001}
{"type": "FILE_LIST", "node_id": "3f9a1c", "files": ["a.txt"]}
{"type": "DATA_REQUEST", "node_id": "3f9a1c", "filename": "a.txt"}
{"type": "FILE_DATA", "node_id": "3f9a1c", "filename": "a.txt", "status": "OK", "content": "aGVsbG8="}
{"type": "FILE_DATA", "node_id": "3f9a1c", "filename": "x.txt", "status": "NOT_FOUND", "content": null}
```

## Division of work

- Naozumi Kawai: `discovery.py` - discovery, connections, and peer table.
- Brian Ou: `protocol.py` - messages, framing, receive loop, and logging.
- Richard Yip: `agent.py` - main, file transfer, periodic file lists, and commands.

## Execution example



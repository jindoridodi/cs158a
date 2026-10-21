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

The following excerpts are from a five-node run on one machine using TCP ports
6101–6105. Each node started with its own `nodeN.txt` in `shared/`. Node IDs,
addresses, and timestamps change on each run.

Node 1 discovered all four other nodes and received their file lists:

```text
[2026-10-20 20:57:30] SENT <broadcast>:54321 PEER_REQUEST
[2026-10-20 20:57:32] RECEIVED 192.168.4.51:54321 PEER_ACK
[2026-10-20 20:57:32] ADDED e87e02bb44bc4e3e873f3c3ba84dac3a 192.168.4.51:6103
[2026-10-20 20:57:32] ADDED 637204bd6ff04494b29f63bd650c7f3d 192.168.4.51:6104
[2026-10-20 20:57:32] ADDED 17bc08e1553d442098af8a1c73635686 192.168.4.51:6102
[2026-10-20 20:57:32] ADDED dcf8cc82571f44c794a56d14c43f1437 192.168.4.51:6105
[2026-10-20 20:57:32] RECEIVED 192.168.4.51:6105 FILE_LIST
```

Node 5 requested `node1.txt` from Node 1 and saved it in both `downloads/`
and `shared/`:

```text
[2026-10-20 20:57:34] SENT 192.168.4.51:54114 DATA_REQUEST
Requested node1.txt from a27083c9.
[2026-10-20 20:57:34] RECEIVED 192.168.4.51:54114 FILE_DATA
[2026-10-20 20:57:34] DOWNLOAD node1.txt from a27083c9: complete
Downloaded node1.txt from a27083c9.
```

Nodes send a `FILE_LIST` over TCP to each connected peer every 10 seconds.

After the next update, Node 1's `files` command showed that Node 5 now
advertised the downloaded file:

```text
[2026-10-20 20:57:41] RECEIVED 192.168.4.51:6105 FILE_LIST
dcf8cc82 (192.168.4.51:6105):
  node1.txt
  node5.txt
```

When Node 2 quit, Node 1 removed it. The subsequent `peers` and `files`
outputs listed only Nodes 3, 4, and 5:

```text
[2026-10-20 20:57:45] REMOVED 17bc08e1553d442098af8a1c73635686 192.168.4.51:6102
e87e02bb	192.168.4.51:6103
637204bd	192.168.4.51:6104
dcf8cc82	192.168.4.51:6105
```

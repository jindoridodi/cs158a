# PA2 - Leader Election

This program implements a leader election algorithm over an asynchronous ring using TCP sockets.

- Generates a unique UUID using `uuid.uuid4()`
- Acts as both a server and a client
- Receives messages from one neighbor as a server
- Sends messages to the next neighbor as a client
- Participates in a leader election where the process with the largest UUID becomes the leader

## How to run:
Configure ```config.txt``` into this format:
```text
server_ip,server_port
neighbor_ip,neighbor_port
```
Type in terminal ```python myleprocess.py```

## Demo

### Node 1
![Node 1 Terminal](/pa2/readmeassets/image.png)

### Node 2
![Node 2 Terminal](/pa2/readmeassets/image-1.png)

### Node 3
![Node 3 Terminal](/pa2/readmeassets/image-2.png)
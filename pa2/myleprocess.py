import uuid
import threading


# Message Class
class Message:
    def __init__(self, uuid, flag=0):
        self.uuid = uuid
        self.flag = flag


# Node
class Node:
    def __init__(self, server_ip, server_port, neighbor_ip, neighbor_port):
        self.uuid = uuid.uuid4()
        self.flag = 0
        self.leader_id = None
        self.server_ip = server_ip
        self.server_port = server_port
        self.neighbor_ip = neighbor_ip
        self.neighbor_port = neighbor_port


# Server
def server():
    pass


# Client
def client():
    pass

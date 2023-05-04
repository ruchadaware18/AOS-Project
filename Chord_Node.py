import socket
import threading
import time
import hashlib
import random
import sys
from copy import deepcopy

m = 7

class DataStore:
    def __init__(self):
        self.data = {}
    
    def add(self, key, value):
        self.data[key] = value
    
    def remove(self, key):
        del self.data[key]
    
    def find(self, search_key):
        if search_key in self.data:
            return self.data[search_key]
        else:
            print('Data not found!')
            print(self.data)
            return None
        
class NodeInfo:
    def __init__(self, ip_address, port_number):
        self.ip_address = ip_address
        self.port_number = port_number
    
    def __str__(self):
        return f"{self.ip_address}:{self.port_number}"
    class Node:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = int(port)
        self.nodeinfo = NodeInfo(ip, port)
        self.id = self.hash(str(self.nodeinfo))
        self.predecessor = None
        self.successor = None
        self.finger_table = FingerTable(self.id)
        self.data_store = DataStore()
        self.request_handler = RequestHandler()
    
    def hash(self, message):
        # This function finds the id of a string and returns its position in the ring
        digest = hashlib.sha256(message.encode()).hexdigest()
        digest = int(digest, 16) % pow(2,m)
        return digest

    def process_requests(self, message):
        # Manages different requests coming to a node and calls the required function accordingly
        operation = message.split("|")[0]
        args = []
        if len(message.split("|")) > 1:
            args = message.split("|")[1:]
        result = "Done"
        if operation == 'insert_server':
            data = message.split('|')[1].split(":") 
            key = data[0]
            value = data[1]
            self.data_store.insert(key, value)
            result = 'Inserted'

        if operation == "delete_server":
            data = message.split('|')[1]
            self.data_store.data.pop(data)
            result = 'Deleted'

        if operation == "search_server":
            data = message.split('|')[1]
            if data in self.data_store.data:
                return self.data_store.data[data]
            else:
                return "NOT FOUND"
            
        if operation == "send_keys":
            id_of_joining_node = int(args[0])
            result = self.send_keys(id_of_joining_node)

        if operation == "insert":
            data = message.split('|')[1].split(":") 
            key = data[0]
            value = data[1]
            result = self.insert_key(key,value)


        if operation == "delete":
            data = message.split('|')[1]
            result = self.delete_key(data)


        if operation == 'search':
            data = message.split('|')[1]
            result = self.search_key(data)
        
    
        
        if operation == "join_request":
            result  = self.join_request_from_other_node(int(args[0]))

        if operation == "find_predecessor":
            result = self.find_predecessor(int(args[0]))

        if operation == "find_successor":
            result = self.find_successor(int(args[0]))

        if operation == "get_successor":
            result = self.get_successor()

        if operation == "get_predecessor":
            result = self.get_predecessor()

        if operation == "get_id":
            result = self.get_id()

        if operation == "notify":
            self.notify(int(args[0]),args[1],args[2])
    
        return str(result)

    def serve_client_requests(self, conn, addr):
        '''
        The serve_client_requests function listens to incoming requests from clients on the open port and then replies to them. 
        It takes as arguments the connection and the address of the connected client device. 
        '''
        with conn:
            # print('Connected by', addr)
            
            data = conn.recv(1024)
            
            data = str(data.decode('utf-8'))
            data = data.strip('\n')
            # print(data)
            data = self.process_request(data)
            # print('Sending', data)
            data = bytes(str(data), 'utf-8')
            conn.sendall(data)
    
    def start_node(self):
        '''
        The start_node function creates 3 threads for each node:
        On the 1st thread the stabilize_node function is being called repeatedly in a definite interval of time
        On the 2nd thread the fix_finger_table function is being called repeatedly in a definite interval of time
        and on the 3rd thread the serve_client_requests function is running which is continuously listening for any new
        incoming client requests
        '''
        thread_for_stabilize_node = threading.Thread(target=self.stabilize_node)
        thread_for_stabilize_node.start()
        thread_for_fix_finger_table = threading.Thread(target=self.fix_finger_table)
        thread_for_fix_finger_table.start()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.nodeinfo.ip, self.nodeinfo.port))
            s.listen()
            while True:
                conn, addr = s.accept()
                t = threading.Thread(target=self.serve_client_requests, args=(conn, addr))
                t.start()   

    def insert_key_value_pair(self, key, value):
        '''
        The function to handle the incoming key_value pair insertion request from the client. 
        This function searches for the correct node on which the key_value pair needs to be stored and then sends a message 
        to that node to store the key_val pair in its data_store.
        '''
        id_of_key = self.hash(str(key))
        succ = self.find_successor(id_of_key)
        # print("Succ found for inserting key", id_of_key, succ)
        ip, port = self.get_ip_port(succ)
        self.request_handler.send_message(ip, port, "insert_key_value_pair|" + str(key) + ":" + str(value))
        return "Inserted at node id " + str(Node(ip, port).id) + ", key was " + str(key) + ", key hash was " + str(id_of_key)  

    def delete_key_value_pair(self, key):
        '''
        The function to handle the incoming key_value pair deletion request from the client. 
        This function searches for the correct node on which the key_value pair is stored and then sends a message to that node 
        to delete the key_val pair in its data_store.
        '''
        id_of_key = self.hash(str(key))
        succ = self.find_successor(id_of_key)
        # print("Succ found for deleting key", id_of_key, succ)
        ip, port = self.get_ip_port(succ)
        self.request_handler.send_message(ip, port, "delete_key_value_pair|" + str(key))
        return "Deleted at node id " + str(Node(ip, port).id) + ", key was " + str(key) + ", key hash was " + str(id_of_key)

    def search_key_value_pair(self, key):
        id_of_key = self.hash(str(key))
        succ = self.find_successor(id_of_key)
        # print("Succ found for searching key" , id_of_key , succ)
        ip,port = self.get_ip_port(succ)
        data = self.request_handler.send_message(ip,port,"search_server|" + str(key) )
        return data


def join_request(self, node_id):
    """Returns the successor node for the node requesting to join."""
    return self.find_successor(node_id)

def join(self, node_ip, node_port):
    '''
    This function is responsible for adding new nodes to the Chord ring. It finds the successor and predecessor of the
    new incoming node in the ring and then sends a send_keys request to its successor to receive all the keys 
    smaller than its id from its successor.
    '''
    data = 'join_request|' + str(self.id)
    succ = self.request_handler.send_message(node_ip, node_port, data)
    ip, port = self.get_ip_port(succ)
    self.successor = Node(ip, port)
    self.finger_table.table[0][1] = self.successor
    self.predecessor = None
    
    if self.successor.id != self.id:
        data = self.request_handler.send_message(self.successor.ip, self.successor.port, "send_keys|" + str(self.id))
        for key_value in data.split(':'):
            if len(key_value) > 1:
                self.data_store.data[key_value.split('|')[0]] = key_value.split('|')[1]

def find_predecessor(self, search_id):
    '''
    This function provides the predecessor of any value in the ring given its id.
    '''
    if search_id == self.id:
        return str(self.nodeinfo)
    if self.predecessor is not None and self.successor.id == self.id:
        return self.nodeinfo.__str__()
    if self.get_forward_distance(self.successor.id) > self.get_forward_distance(search_id):
        return self.nodeinfo.__str__()
    else:
        new_node_hop = self.closest_preceding_node(search_id)
        if new_node_hop is None:
            return "None"
        ip, port = self.get_ip_port(str(new_node_hop))
        if ip == self.ip and port == self.port:
            return self.nodeinfo.__str__()
        data = self.request_handler.send_message(ip, port, "find_predecessor|" + str(search_id))
        return data

def find_successor(self, search_id):
    '''
    This function provides the successor of any value in the ring given its id.
    '''
    if search_id == self.id:
        return str(self.nodeinfo)
    predecessor = self.find_predecessor(search_id)
    if predecessor == "None":
        return "None"
    ip, port = self.get_ip_port(predecessor)
    data = self.request_handler.send_message(ip, port, "get_successor")
    return data

def closest_preceding_node(self, search_id):
    closest_node = None
    min_distance = pow(2, m) + 1
    for i in list(reversed(range(m))):
        if self.finger_table.table[i][1] is not None and self.get_forward_distance_2nodes(self.finger_table.table[i][1].id, search_id) < min_distance:
            closest_node = self.finger_table.table[i][1]
            min_distance = self.get_forward_distance_2nodes(self.finger_table.table[i][1].id, search_id)
    return closest_node

def send_keys(self, id_of_joining_node):
    '''
    This function is used to send all the keys less than or equal to the id_of_joining_node to the new node that
    has joined the Chord ring.
    '''
    data = ""
    keys_to_be_removed = []
    for keys in self.data_store.data:
        key_id = self.hash(str(keys))
        if self.get_forward_distance_2nodes(key_id , id_of_joining_node) < self.get_forward_distance_2nodes(key_id,self.id):
            data += str(keys) + "|" + str(self.data_store.data[keys]) + ":"
            keys_to_be_removed.append(keys)
    for keys in keys_to_be_removed:
        self.data_store.data.pop(keys)
    return data


def stabilize(self):
    '''
    The stabilize function is called in regular intervals to ensure that each node is pointing to its correct successor 
    and predecessor nodes. It also updates the node's information of new nodes joining the ring.
    '''
    while True:
        if self.successor is None:
            time.sleep(10)
            continue
        data = "get_predecessor"

        if self.successor.ip == self.ip and self.successor.port == self.port:
            time.sleep(10)
        result = self.request_handler.send_message(self.successor.ip, self.successor.port, data)
        if result == "None" or len(result) == 0:
            self.request_handler.send_message(self.successor.ip, self.successor.port, "notify|" + str(self.id) + "|" + self.nodeinfo.__str__())
            continue

        ip, port = self.get_ip_port(result)
        result = int(self.request_handler.send_message(ip, port, "get_id"))
        if self.get_backward_distance(result) > self.get_backward_distance(self.successor.id):
            self.successor = Node(ip, port)
            self.finger_table.table[0][1] = self.successor
        self.request_handler.send_message(self.successor.ip, self.successor.port, "notify|" + str(self.id) + "|" + self.nodeinfo.__str__())
        print("=" * 50)
        print("STABILIZING")
        print("=" * 50)
        print("ID:", self.id)
        if self.successor is not None:
            print("Successor ID:", self.successor.id)
        if self.predecessor is not None:
            print("Predecessor ID:", self.predecessor.id)
        print("=" * 50)
        print("FINGER TABLE")
        self.finger_table.print()
        print("=" * 50)
        print("DATA STORE")
        print("=" * 50)
        print(str(self.data_store.data))
        print("=" * 50)
        print("END")
        print()
        print()
        print()
        time.sleep(10)

def notify(self, node_id, node_ip, node_port):
    '''
    Receives notification from the stabilize function when there is a change in successor.
    '''
    if self.predecessor is not None:
        if self.get_backward_distance(node_id) < self.get_backward_distance(self.predecessor.id):
            self.predecessor = Node(node_ip, int(node_port))
            return
    if self.predecessor is None or self.predecessor == "None" or (node_id > self.predecessor.id and node_id < self.id) or (self.id == self.predecessor.id and node_id != self.id):
        self.predecessor = Node(node_ip, int(node_port))
        if self.id == self.successor.id:
            self.successor = Node(node_ip, int(node_port))
            self.finger_table.table[0][1] = self.successor

def fix_fingers(self):
    '''
    The fix_fingers function is used to correct the finger table at regular intervals. It waits for 10 seconds and then 
    picks one random index of the table and corrects it so that if any new node has joined the ring it can properly mark 
    that node in its finger table.
    '''
    while True:
        while True:

            random_index = random.randint(1,m-1)
            finger = self.finger_table.table[random_index][0]
            # print("in fix fingers , fixing index", random_index)
            data = self.find_successor(finger)
            if data == "None":
                time.sleep(10)
                continue
            ip,port = self.get_ip_port(data)
            self.finger_table.table[random_index][1] = Node(ip,port) 
            time.sleep(10)

class Node:
    def __init__(self, id, nodeinfo=None, predecessor=None, successor=None):
        self.id = id
        self.nodeinfo = nodeinfo
        self.predecessor = predecessor
        self.successor = successor

    def get_successor(self):
        '''
        This function returns the successor of the node.
        If the successor is None, it returns "None" as a string.
        '''
        if self.successor is None:
            return "None"
        return str(self.successor.nodeinfo)

    def get_predecessor(self):
        '''
        This function returns the predecessor of the node.
        If the predecessor is None, it returns "None" as a string.
        '''
        if self.predecessor is None:
            return "None"
        return str(self.predecessor.nodeinfo)

    def get_id(self):
        '''
        This function returns the ID of the node as a string.
        '''
        return str(self.id)

    def get_ip_port(self, string_format):
        '''
        This function returns a tuple containing the IP address and port number of the node.
        The input string_format should be of the format "IP_address|port_number".
        '''
        ip_address, port_number = string_format.strip().split('|')
        return ip_address, int(port_number)

    def get_backward_distance(self, node1):
        '''
        This function returns the backward distance between the current node and node1.
        The backward distance is the distance from the current node to node1 going clockwise.
        '''
        if self.id > node1:
            distance = self.id - node1
        elif self.id == node1:
            distance = 0
        else:
            distance = pow(2, m) - abs(self.id - node1)
        return distance

    def get_backward_distance_2nodes(self, node2, node1):
        '''
        This function returns the backward distance between node2 and node1.
        The backward distance is the distance from node2 to node1 going clockwise.
        '''
        if node2 > node1:
            distance = node2 - node1
        elif node2 == node1:
            distance = 0
        else:
            distance = pow(2, m) - abs(node2 - node1)
        return distance

    def get_forward_distance(self, nodeid):
        '''
        This function returns the forward distance between the current node and nodeid.
        The forward distance is the distance from the current node to nodeid going counterclockwise.
        '''
        return pow(2, m) - self.get_backward_distance(nodeid)

    def get_forward_distance_2nodes(self, node2, node1):
        '''
        This function returns the forward distance between node2 and node1.
        The forward distance is the distance from node2 to node1 going counterclockwise.
        '''
        return pow(2, m) - self.get_backward_distance_2nodes(node2, node1)
    

class FingerTable:
    '''
    The FingerTable class represents the finger table of a node in a Chord
    distributed hash table.

    Attributes:
        my_id (int): The id of the node that owns this finger table.
        table (list of tuples): The finger table entries. Each entry is a tuple
            of the form (start, successor), where `start` is the start of the
            interval for which this node is responsible, and `successor` is the
            successor node for that interval.
    '''

    def __init__(self, my_id):
        '''
        Initializes a new finger table for the given node id.

        Args:
            my_id (int): The id of the node that owns this finger table.
        '''
        self.my_id = my_id
        self.table = [(self._get_finger(i), None) for i in range(m)]

    def _get_finger(self, i):
        '''
        Returns the i-th finger for this node.

        Args:
            i (int): The index of the finger to return.

        Returns:
            int: The id of the node that is responsible for the i-th finger
                interval.
        '''
        return (self.my_id + pow(2, i)) % pow(2, m)

    def __str__(self):
        '''
        Returns a string representation of this finger table.

        Returns:
            str: A string representation of this finger table.
        '''
        lines = []
        for i, (start, successor) in enumerate(self.table):
            if successor is None:
                successor_id = "None"
            else:
                successor_id = str(successor.id)
            lines.append(f"Entry: {i}, Interval start: {start}, Successor: {successor_id}")
        return "\n".join(lines)

class RequestHandler:
    def __init__(self):
        pass
    def send_message(self, ip, port, message):
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
  
        # connect to server on local computer 
        s.connect((ip,port)) 
        s.send(message.encode('utf-8')) 
        data = s.recv(1024) 
        s.close()
        return data.decode("utf-8") 
        

# The ip = "127.0.0.1" signifies that the node is executing on the localhost

ip = "127.0.0.1"
# This if statement is used to check if the node joining is the first node of the ring or not

if len(sys.argv) == 3:
    print("JOINING RING")
    node = Node(ip, int(sys.argv[1]))

    node.join(ip,int(sys.argv[2]))
    node.start()

if len(sys.argv) == 2:
    print("CREATING RING")
    node = Node(ip, int(sys.argv[1]))

    node.predecessor = Node(ip,node.port)
    node.successor = Node(ip,node.port)
    node.finger_table.table[0][1] = node
    node.start()



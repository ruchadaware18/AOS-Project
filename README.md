# AOS-Project
Advanced Operating Systems Project on Chord Protocol
There are two files which work for nodes entering inside the ring. The Helper.py has some helping functions for the Node. And the Chord_Node.py has the Chord Protocol implementation which is based on the algorithm. The files are:
- Helper.py
- Chord_Node.py

To run the code, run the following commands
```
python Chord_Node.py node_port_number   // Creating new node and ring
python Chord_Node.py New_port_number old_port_number  // joining existing ring
python3 Client.py node_port_number
```

Example : 
```
python Chord_Node.py 9090   // Creating new node and ring
python Chord_Node.py 8088 9090  // joining existing ring
python3 Client.py 9090
```

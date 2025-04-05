# RAFT Key-Value Store

This project simulates a distributed key-value store built on the RAFT consensus algorithm. It includes leader election, heartbeat messaging, and replication of data across nodes.

## 🚀 How to Run the Project

Follow these steps to get the system up and running:

### Prerequisites

Python 3.7+
gRPC
Protocol Buffers compiler

### 2. Installation

Clone the repository
Install the required dependencies:

```
pip install grpc grpcio-tools
```

### 3. Compile the gRPC definitions

Run the following command to generate Python files from the `.proto` file:

```bash
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto
```

### 4. Start the RAFT cluster

##### Running the Cluster
The cluster can be easily started using the provided script:

```bash
python3 start_cluster.py
```

This automatically starts a 3-node RAFT cluster with dynamic port allocation, leader election, and monitoring.

You can also specify a custom number of nodes:

```bash
python3 start_cluster.py <number_of_nodes>
```

##### Cluster Management

The start_cluster.py script provides an interactive interface for managing the cluster:

status - Display current cluster status and leader information
kill <node_id> - Terminate a specific node
kill leader - Terminate the current leader node (triggers new election)
restart <node_id> - Restart a previously killed node
quit - Exit the program and terminate all nodes

Once all nodes are running, the system will elect a leader and begin exchanging heartbeat messages.

### 5. Run the client

After starting the cluster, run the client in a separate terminal:

```bash
Copypython3 client.py
```
The client will:

Automatically discover the current leader
Allow you to enter key-value pairs mutiple times to store in the distributed system
Handle leader failures by reconnecting to the new leader

Enter a key and value when prompted. The client will send this data to the leader, which will replicate it to the follower nodes.

### 6. Check the log files

You can verify system operations by checking the log files:

- `client.txt`
- `node1.txt`, `node2.txt`, `node3.txt`, etc.

These logs will show events such as heartbeats, data replication, and elections.

### 7. Log Files
The system generates the following log files:

client.txt: Records client operations and leader discovery
node1.txt, node2.txt, etc.: Record node operations, elections, and data replication
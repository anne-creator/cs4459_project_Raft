# RAFT Key-Value Store

This project simulates a distributed key-value store built on the RAFT consensus algorithm. It includes leader election, heartbeat messaging, and replication of data across nodes.

## 🚀 How to Run the Project

Follow these steps to get the system up and running:

### 1. Clone the repository

```bash
git clone <repo-url>
```

### 2. Pull the latest changes

Make sure you have the most recent updates:

```bash
git fetch && git pull
```

### 3. Compile the gRPC definitions

Run the following command to generate Python files from the `.proto` file:

```bash
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto
```

### 4. Start the RAFT cluster

Open **three terminal windows**, and in each one, run the following command (replacing `X` with the node number):

```bash
python3 raft_node.py nodeX
```

For example:

```bash
python3 raft_node.py node1
python3 raft_node.py node2
python3 raft_node.py node3
```

> **Note:** It's important to start the nodes within a short timeframe to avoid unnecessary elections due to timeouts. The first node started will most likely become the leader.

Once all nodes are running, the system will elect a leader and begin exchanging heartbeat messages.

### 5. Run the client

In a **fourth terminal**, run the client:

```bash
python3 client.py
```

Enter a key and value when prompted. The client will send this data to the leader, which will replicate it to the follower nodes.

### 6. Check the log files

You can verify system operations by checking the log files:

- `client.txt`
- `node1.txt`, `node2.txt`, `node3.txt`, etc.

These logs will show events such as heartbeats, data replication, and elections.

### 7. Test leader failure

If you terminate the leader node, the remaining nodes will automatically detect the failure and elect a new leader.

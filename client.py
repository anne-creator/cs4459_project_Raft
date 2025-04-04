import grpc
import json
import time
import os
import raft_pb2
import raft_pb2_grpc
from google.protobuf.empty_pb2 import Empty

def initialize_log():
    """Delete existing log file and create a new one."""
    if os.path.exists("client.txt"):
        os.remove("client.txt")
    log("Client started")

def log(message):
    """Write a message to the client log file."""
    with open("client.txt", "a") as f:
        f.write(f"{time.ctime()} — {message}\n")

def discover_leader(nodes_config):
    """Find the current leader by querying all nodes."""
    for node_id, info in nodes_config.items():
        try:
            with grpc.insecure_channel(info["address"]) as channel:
                stub = raft_pb2_grpc.RaftStub(channel)
                response = stub.WhoIsLeader(Empty(), timeout=2.0)
                if response.address:
                    log(f"Discovered leader at {response.address}")
                    return response.address
        except grpc.RpcError:
            continue
    log("No leader found")
    return None

def send_to_leader(address, key, value, config):
    """Send a key-value pair to the leader node."""
    with grpc.insecure_channel(address) as channel:
        stub = raft_pb2_grpc.RaftStub(channel)
        try:
            response = stub.Put(raft_pb2.PutRequest(key=key, value=value), timeout=2.0)
            if response.success:
                log(f"Successfully stored ({key}, {value}) at leader {address}")
                print(f"Successfully stored ({key}, {value})!")
                return True
            else:
                log(f"{address} rejected write")
                print(f"Leader rejected the write request.")
                return False
        except grpc.RpcError as e:
            log(f"Failed to send to leader {address}: {e}")
            
    # Only reach here if there was an exception
    try:
        # Re-discover and retry once
        new_address = discover_leader(config)
        if new_address and new_address != address:
            log(f"Retrying with new leader at {new_address}")
            print(f"Retrying with new leader...")
            with grpc.insecure_channel(new_address) as channel:
                stub = raft_pb2_grpc.RaftStub(channel)
                response = stub.Put(raft_pb2.PutRequest(key=key, value=value), timeout=2.0)
                if response.success:
                    log(f"Successfully stored ({key}, {value}) at new leader {new_address}")
                    print(f"Successfully stored ({key}, {value})!")
                    return True
    except grpc.RpcError as e:
        log(f"Retry failed: {e}")
    
    print("Failed to store data. Please try again.")
    return False

def main():
    """Main client function with interactive loop."""
    initialize_log()
    
    try:
        with open("nodes_config.json") as f:
            config = json.load(f)
    except FileNotFoundError:
        print("Error: nodes_config.json file not found")
        log("Error: nodes_config.json file not found")
        return
    
    print("RAFT Client - Enter 'quit' for key to exit")
    
    while True:
        # Get key-value from user
        key = input("\nEnter key: ")
        if key.lower() == 'quit':
            print("Exiting client.")
            log("Client exiting")
            break
            
        value = input("Enter value: ")
        
        # Find leader and send data
        leader_address = discover_leader(config)
        if leader_address:
            send_to_leader(leader_address, key, value, config)
        else:
            print("Could not find a leader. Please try again later.")
            log("No leader available for write operation")

if __name__ == "__main__":
    main()
import grpc
import json
import time
import raft_pb2
import raft_pb2_grpc

def log(message):
    with open("client.txt", "a") as f:
        f.write(f"{time.ctime()} — {message}\n")

def discover_leader(nodes_config):
    for node_id, info in nodes_config.items():
        try:
            with grpc.insecure_channel(info["address"]) as channel:
                stub = raft_pb2_grpc.RaftStub(channel)
                stub.AppendEntries(raft_pb2.AppendEntriesRequest(term=0, leader_id=""), timeout=2.0)
                return info["address"]
        except grpc.RpcError:
            continue
    return None

def send_to_leader(address, key, value):
    with grpc.insecure_channel(address) as channel:
        stub = raft_pb2_grpc.RaftStub(channel)
        try:
            response = stub.Put(raft_pb2.PutRequest(key=key, value=value), timeout=2.0)
            if response.success:
                log(f"Sent ({key}, {value}) to leader at {address}")
                print(f"Stored ({key}, {value}) successfully!")
            else:
                log(f"Leader {address} rejected the write.")
                print(f"Leader {address} rejected the write.")
        except grpc.RpcError as e:
            log(f"Failed to send to leader: {e}")
            print("Failed to send to leader.")

if __name__ == "__main__":
    with open("nodes_config.json") as f:
        config = json.load(f)

    key = input("Enter key: ")
    value = input("Enter value: ")
    leader_address = discover_leader(config)

    if leader_address:
        send_to_leader(leader_address, key, value)
    else:
        print("Could not find leader")
        log("Could not find leader")

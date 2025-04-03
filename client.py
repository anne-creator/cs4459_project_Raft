import grpc
import json
import time
import raft_pb2
import raft_pb2_grpc
from google.protobuf.empty_pb2 import Empty

def log(message):
    with open("client.txt", "a") as f:
        f.write(f"{time.ctime()} — {message}\n")

def discover_leader(nodes_config):
    for node_id, info in nodes_config.items():
        try:
            with grpc.insecure_channel(info["address"]) as channel:
                stub = raft_pb2_grpc.RaftStub(channel)
                response = stub.WhoIsLeader(Empty(), timeout=2.0)
                if response.address:
                    return response.address
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
                return
            else:
                log(f"{address} rejected write — retrying with new leader...")
        except grpc.RpcError as e:
            log(f"Failed to send to leader {address}: {e}")
    
    # Re-discover and retry
    new_address = discover_leader(config)
    if new_address:
        print(new_address)
        send_to_leader(new_address, key, value)
    else:
        log("Retry failed — no new leader found.")
        print("Retry failed — no new leader found.")


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

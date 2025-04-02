import os
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

import sys, json, time, threading, grpc
from concurrent.futures import ThreadPoolExecutor
from google.protobuf import empty_pb2

import replication_pb2
import replication_pb2_grpc
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc

# Class for single node in dis system
class SequenceServicer(replication_pb2_grpc.SequenceServicer):
    def __init__(self, node_id, config_path="config.json"):
        self.id = node_id
        self.state = "follower"
        self.current_term = 0
        self.voted_for = None
        self.data = {}
        self.log_file = f"{self.id}.txt"

        # Clears the log file on startup
        open(self.log_file, "w").close()

        # Load config and set up peers
        with open(config_path) as f:
            config = json.load(f)

        if self.id not in config:
            raise ValueError(f"{self.id} not found in {config_path}")

        self.host = config[self.id]["host"]
        self.port = config[self.id]["port"]

        # Get other peers
        self.peers = {}
        for peer_id, info in config.items():
            if peer_id != self.id:
                self.peers[peer_id] = info

        self.peer_stubs = {}
        for peer_id, info in self.peers.items():
            address = f"{info['host']}:{info['port']}"
            self.peer_stubs[peer_id] = replication_pb2_grpc.SequenceStub(
                grpc.insecure_channel(address)
            )

        print(f"[{self.id}] {self.state} started on {self.host}:{self.port} | Peers: {list(self.peers)}")

    def Write(self, request, context):
        key = request.key
        value = request.value
        print(f"[{self.id}] Received write request: {key}:{value}")

        # Leader forwards write to peers
        if self.state == "leader":
            for peer_id, stub in self.peer_stubs.items():
                try:
                    stub.Write(replication_pb2.WriteRequest(key=key, value=value))
                except grpc.RpcError as e:
                    print(f"[{self.id}] Failed to send to {peer_id}: {e}")

        self.data[key] = value
        with open(self.log_file, "a") as log_file:
            log_file.write(f"{key} {value}\n")

        return replication_pb2.WriteResponse(ack="true")

    def send_heartbeats(self):
        def heartbeat_loop():
            retry_delay = 5
            while True:
                try:
                    with grpc.insecure_channel("localhost:50053") as channel:
                        stub = heartbeat_service_pb2_grpc.ViewServiceStub(channel)
                        print(f"[{self.id}] Connected to heartbeat server")
                        while True:
                            stub.Heartbeat(heartbeat_service_pb2.HeartbeatRequest(service_identifier=self.id))
                            print(f"[{self.id}] Sent heartbeat")
                            time.sleep(5)
                except:
                    print(f"[{self.id}] Heartbeat connection failed. Retrying...")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 30)

        threading.Thread(target=heartbeat_loop, daemon=True).start()

    def serve(self):
        server = grpc.server(ThreadPoolExecutor(max_workers=10))
        replication_pb2_grpc.add_SequenceServicer_to_server(self, server)
        server.add_insecure_port(f"{self.host}:{self.port}")
        server.start()
        print(f"[{self.id}] gRPC server running at {self.host}:{self.port}")
        return server

def main():
    if len(sys.argv) != 2:
        print("Usage: python server.py <node_id>")
        sys.exit(1)

    node_id = sys.argv[1]
    node = SequenceServicer(node_id, config_path="config.json")
    node.send_heartbeats()
    grpc_server = node.serve()

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        grpc_server.stop(0)
        print(f"[{node.id}] Server stopped")

if __name__ == "__main__":
    main()

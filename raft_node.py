# raft_node.py (with Put method for key-value storage)
import grpc
import threading
import time
import random
import json
from concurrent import futures
from google.protobuf.empty_pb2 import Empty
from raft_pb2 import RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse, LeaderNotification, PutRequest, PutResponse
import raft_pb2_grpc

FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"

class RaftNode(raft_pb2_grpc.RaftServicer):
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers  # List of (id, address)
        self.current_term = 0
        self.voted_for = None
        self.state = FOLLOWER
        self.lock = threading.Lock()
        self.votes_received = 0
        self.leader_id = None
        self.election_timeout = self._reset_election_timeout()
        self.log_file = f"{self.node_id}.txt"
        self.store = {}

    def log(self, message):
        with open(self.log_file, "a") as f:
            f.write(f"{time.ctime()} — {message}\n")

    def _reset_election_timeout(self):
        return time.time() + random.uniform(5, 6)

    def _start_election(self):
        print(f"{self.node_id}: _start_election() called!")
        with self.lock:
            self.state = CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.votes_received = 1
            self.election_timeout = self._reset_election_timeout()
        self.log(f"Starting election for term {self.current_term}")
        for peer_id, address in self.peers:
            threading.Thread(target=self._request_vote, args=(peer_id, address)).start()

    def _request_vote(self, peer_id, address):
        with grpc.insecure_channel(address) as channel:
            stub = raft_pb2_grpc.RaftStub(channel)
            try:
                response = stub.RequestVote(
                    RequestVoteRequest(
                        term=self.current_term,
                        candidate_id=self.node_id
                    ),
                    timeout=2.0
                )
                with self.lock:
                    if response.vote_granted:
                        self.votes_received += 1
                        self.log(f"Got vote from {peer_id}")
                        if self.votes_received > len(self.peers) // 2 and self.state == CANDIDATE:
                            self.state = LEADER
                            self.leader_id = self.node_id
                            self.log(f"Became leader for term {self.current_term}")
                            self._notify_others_of_leader()
            except grpc.RpcError as e:
                self.log(f"Failed to contact {peer_id}: {e}")

    def _notify_others_of_leader(self):
        for peer_id, address in self.peers:
            try:
                with grpc.insecure_channel(address) as channel:
                    stub = raft_pb2_grpc.RaftStub(channel)
                    stub.NotifyLeader(LeaderNotification(leader_id=self.node_id), timeout=2.0)
                    self.log(f"Notified {peer_id} of new leader")
            except grpc.RpcError as e:
                self.log(f"Failed to notify {peer_id} about leader: {e}")

    def RequestVote(self, request, context):
        with self.lock:
            if request.term < self.current_term:
                return RequestVoteResponse(term=self.current_term, vote_granted=False)
            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None
                self.state = FOLLOWER
            if self.voted_for is None or self.voted_for == request.candidate_id:
                self.voted_for = request.candidate_id
                self.election_timeout = self._reset_election_timeout()
                return RequestVoteResponse(term=self.current_term, vote_granted=True)
            return RequestVoteResponse(term=self.current_term, vote_granted=False)

    def AppendEntries(self, request, context):
        with self.lock:
            if request.term >= self.current_term:
                self.current_term = request.term
                self.leader_id = request.leader_id
                self.state = FOLLOWER
                self.election_timeout = self._reset_election_timeout()
                self.log(f"Received heartbeat from leader {request.leader_id}")
                return AppendEntriesResponse(term=self.current_term, success=True)
            return AppendEntriesResponse(term=self.current_term, success=False)

    def NotifyLeader(self, request, context):
        with self.lock:
            self.leader_id = request.leader_id
            self.state = FOLLOWER
            self.log(f"Notified of new leader {self.leader_id}")
        return Empty()

    def Put(self, request, context):
        if self.state != LEADER:
            self.log("Put request rejected — not the leader")
            return PutResponse(success=False)
        self.store[request.key] = request.value
        self.log(f"Stored: {request.key} → {request.value}")
        return PutResponse(success=True)

    def start(self, port):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServicer_to_server(self, server)
        server.add_insecure_port(f'localhost:{port}')
        server.start()
        print(f"Node {self.node_id} is running and listening on port {port}...")
        self.log(f"gRPC server started on port {port}")

        threading.Thread(target=self._run).start()
        server.wait_for_termination()

    def _run(self):
        while True:
            now = time.time()
            print(f"{self.node_id} loop running. State: {self.state}")
            # print(f"{self.node_id}: state={self.state}, now={now:.2f}, timeout={self.election_timeout:.2f}")

            if self.state != LEADER and now > self.election_timeout:
                print(f"{self.node_id}: Triggering election logic...")
                self._start_election()

            elif self.state == LEADER:
                for peer_id, address in self.peers:
                    threading.Thread(target=self._send_heartbeat, args=(address,)).start()

            time.sleep(3)

    def _send_heartbeat(self, address):
        with grpc.insecure_channel(address) as channel:
            stub = raft_pb2_grpc.RaftStub(channel)
            try:
                stub.AppendEntries(AppendEntriesRequest(term=self.current_term, leader_id=self.node_id), timeout=2.0)
                self.log(f"Sent heartbeat to {address}")
            except grpc.RpcError as e:
                self.log(f"Failed to send heartbeat to {address}: {e}")

if __name__ == "__main__":
    import sys
    with open("nodes_config.json") as f:
        config = json.load(f)

    this_id = sys.argv[1]
    node_info = config[this_id]
    other_peers = [(peer_id, info["address"]) for peer_id, info in config.items() if peer_id != this_id]

    node = RaftNode(this_id, other_peers)
    node.start(node_info["port"])

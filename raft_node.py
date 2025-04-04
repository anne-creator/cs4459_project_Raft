import grpc
import threading
import time
import random
import json
from concurrent import futures
from google.protobuf.empty_pb2 import Empty
from raft_pb2 import RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse, LeaderNotification, PutRequest, PutResponse, ReplicateRequest, ReplicateResponse, LeaderResponse, LogEntry
import raft_pb2_grpc

FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"

class RaftNode(raft_pb2_grpc.RaftServicer):
    def __init__(self, node_id, peers):
        open(f"{node_id}.txt", "w").close()
        self.node_id = node_id
        self.peers = peers
        self.current_term = 0
        self.voted_for = None
        self.state = FOLLOWER
        self.lock = threading.Lock()
        self.votes_received = 0
        self.leader_id = None
        self.election_timeout = self._reset_election_timeout()
        self.log_file = f"{self.node_id}.txt"
        self.store = {}
        self.log = []
        self.commit_index = -1
        self.last_applied = -1
        self.next_index = {}
        self.match_index = {}

    def write_log(self, message):
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(f"{time.ctime()} — {message}\n")

    def apply_committed_entries(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            self.store[entry["key"]] = entry["value"]
            self.write_log(f"Applied entry to store: {entry}")
            print(f"{self.node_id} applied: {entry}")

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
        self.write_log(f"Starting election for term {self.current_term}")
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
                        self.write_log(f"Got vote from {peer_id}")
                        if self.votes_received > len(self.peers) // 2 and self.state == CANDIDATE:
                            self.state = LEADER
                            self.leader_id = self.node_id
                            self.write_log(f"Became leader for term {self.current_term}")
                            self._notify_others_of_leader()
            except grpc.RpcError as e:
                self.write_log(f"Failed to contact {peer_id}: {e}")

    def _notify_others_of_leader(self):
        for peer_id, address in self.peers:
            self.next_index[peer_id] = len(self.log)
            self.match_index[peer_id] = -1
            try:
                with grpc.insecure_channel(address) as channel:
                    stub = raft_pb2_grpc.RaftStub(channel)
                    stub.NotifyLeader(LeaderNotification(leader_id=self.node_id), timeout=2.0)
                    self.write_log(f"Notified {peer_id} of new leader")
            except grpc.RpcError as e:
                self.write_log(f"Failed to notify {peer_id} about leader: {e}")

    def _get_leader_address(self):
        if self.leader_id:
            for peer_id, addr in self.peers:
                if peer_id == self.leader_id:
                    return addr
            if self.leader_id == self.node_id:
                return f"localhost:{self.port}"
        return ""

    def WhoIsLeader(self, request, context):
        return LeaderResponse(leader_id=self.leader_id or "", address=self._get_leader_address())

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
            if request.term < self.current_term:
                return AppendEntriesResponse(term=self.current_term, success=False)

            self.current_term = request.term
            self.leader_id = request.leader_id
            self.state = FOLLOWER
            self.election_timeout = self._reset_election_timeout()

            if request.prev_log_index >= len(self.log):
                return AppendEntriesResponse(term=self.current_term, success=False)
            if request.prev_log_index >= 0 and self.log[request.prev_log_index]["term"] != request.prev_log_term:
                return AppendEntriesResponse(term=self.current_term, success=False)

            self.log = self.log[:request.prev_log_index + 1]
            for entry in request.entries:
                self.log.append({
                    "index": entry.index,
                    "term": entry.term,
                    "key": entry.key,
                    "value": entry.value
                })

            if request.leader_commit > self.commit_index:
                self.commit_index = min(request.leader_commit, len(self.log) - 1)

            self.apply_committed_entries()

            return AppendEntriesResponse(term=self.current_term, success=True)

    def NotifyLeader(self, request, context):
        with self.lock:
            self.leader_id = request.leader_id
            self.state = FOLLOWER
            self.write_log(f"Notified of new leader {self.leader_id}")
        return Empty()

    def Put(self, request, context):
        if self.state != LEADER:
            self.write_log("Put request rejected — not the leader")
            print(f"{self.node_id} rejected Put request — not the leader")
            return PutResponse(success=False)

        entry = {
            "index": len(self.log),
            "term": self.current_term,
            "key": request.key,
            "value": request.value
        }
        self.log.append(entry)
        self.write_log(f"Appended to log: {entry}")
        print(f"{self.node_id} appended entry to log: {entry}")

        acks = 1
        for peer_id, address in self.peers:
            try:
                with grpc.insecure_channel(address) as channel:
                    stub = raft_pb2_grpc.RaftStub(channel)
                    response = stub.Replicate(
                        ReplicateRequest(key=request.key, value=request.value),
                        timeout=2.0
                    )
                    if response.ack:
                        acks += 1
                        print(f"{self.node_id} received ack from {peer_id}")
            except grpc.RpcError as e:
                self.write_log(f"Replication to {peer_id} failed: {e}")
                print(f"{self.node_id} failed to replicate to {peer_id}")

        if acks > len(self.peers) // 2:
            self.commit_index = entry["index"]
            self.apply_committed_entries()
            self.write_log(f"Put({request.key}) replicated successfully to majority")
            print(f"{self.node_id} Put({request.key}) committed")
            return PutResponse(success=True)
        else:
            self.write_log(f"Put({request.key}) failed to reach majority")
            print(f"{self.node_id} Put({request.key}) failed")
            return PutResponse(success=False)

    def Replicate(self, request, context):
        self.write_log(f"Replicated: {request.key} -> {request.value}")
        print(f"{self.node_id} replicated key={request.key} value={request.value}")
        return ReplicateResponse(ack=True)

    def start(self, port):
        self.port = port
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServicer_to_server(self, server)
        server.add_insecure_port(f'localhost:{port}')
        server.start()
        print(f"Node {self.node_id} is running and listening on port {port}...")
        self.write_log(f"gRPC server started on port {port}")

        threading.Thread(target=self._run, daemon=True).start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"{self.node_id} shutting down.")
            server.stop(0)

    def _run(self):
        while True:
            now = time.time()
            print(f"[{time.strftime('%H:%M:%S')}] {self.node_id} loop running. State: {self.state}")

            if self.state != LEADER and now > self.election_timeout:
                print(f"{self.node_id}: Triggering election logic...")
                self._start_election()

            elif self.state == LEADER:
                for peer_id, address in self.peers:
                    threading.Thread(target=self._send_append_entries, args=(peer_id, address)).start()

            time.sleep(3)
    def _send_append_entries(self, peer_id, address):
        try:
            with grpc.insecure_channel(address) as channel:
                stub = raft_pb2_grpc.RaftStub(channel)
                
                # Initialize next_index for newly seen peers
                if peer_id not in self.next_index:
                    self.next_index[peer_id] = 0
                    self.match_index[peer_id] = -1
                
                prev_index = self.next_index.get(peer_id, 0) - 1
                prev_term = self.log[prev_index]["term"] if prev_index >= 0 else 0
                entries = self.log[self.next_index[peer_id]:]
                
                request = AppendEntriesRequest(
                    term=self.current_term,
                    leader_id=self.node_id,
                    prev_log_index=prev_index,
                    prev_log_term=prev_term,
                    entries=[LogEntry(index=e["index"], term=e["term"], key=e["key"], value=e["value"]) for e in entries],
                    leader_commit=self.commit_index
                )
                
                response = stub.AppendEntries(request, timeout=2.0)
                if response.success:
                    if entries:  # Only update if we sent entries
                        self.match_index[peer_id] = prev_index + len(entries)
                        self.next_index[peer_id] = self.match_index[peer_id] + 1
                        self.write_log(f"Successfully updated {peer_id} with {len(entries)} entries")
                else:
                    # Log consistency check failed, decrement next_index and try again
                    self.next_index[peer_id] = max(0, self.next_index[peer_id] - 1)
                    self.write_log(f"AppendEntries rejected by {peer_id}, trying with lower index {self.next_index[peer_id]}")
        except grpc.RpcError as e:
            self.write_log(f"Failed to send AppendEntries to {peer_id}: {e}")

if __name__ == "__main__":
    import sys
    with open("nodes_config.json") as f:
        config = json.load(f)

    this_id = sys.argv[1]
    node_info = config[this_id]
    other_peers = [(peer_id, info["address"]) for peer_id, info in config.items() if peer_id != this_id]

    node = RaftNode(this_id, other_peers)
    node.start(node_info["port"])
import subprocess
import json
import time
import sys
import os
import signal
import threading

# Global variables
processes = []  # List of (node_id, process, status) tuples
manually_killed = set()  # Set of node_ids that were manually killed
current_leader = None
running = True

def clear_log_files():
    """Clear previous log files"""
    for log_file in ['node1.txt', 'node2.txt', 'node3.txt']:
        if os.path.exists(log_file):
            try:
                os.remove(log_file)
                print(f"Removed previous log file: {log_file}")
            except Exception as e:
                print(f"Warning: Could not remove {log_file}: {e}")

def start_node(node_id, config):
    """Start a single node and return its process"""
    port = config[node_id]["port"]
    cmd = [sys.executable, 'raft_node.py', node_id]
    print(f"Starting {node_id} on port {port}...")
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    return process

def detect_leader():
    """Try to detect the current leader"""
    global current_leader
    
    with open("nodes_config.json") as f:
        config = json.load(f)
    
    for node_id, info in config.items():
        # Skip checking nodes that are manually killed
        if node_id in manually_killed:
            continue
            
        # Find a node that's running to ask about the leader
        node_running = False
        for nid, proc, status in processes:
            if nid == node_id and status == "running" and proc.poll() is None:
                node_running = True
                break
                
        if not node_running:
            continue
            
        try:
            cmd = [
                sys.executable, 
                "-c", 
                f"""
import grpc
import raft_pb2
import raft_pb2_grpc
from google.protobuf.empty_pb2 import Empty
try:
    channel = grpc.insecure_channel('{info["address"]}')
    stub = raft_pb2_grpc.RaftStub(channel)
    response = stub.WhoIsLeader(Empty(), timeout=1.0)
    print(response.leader_id)
except Exception as e:
    pass
                """
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=2)
            
            if result.stdout.strip():
                current_leader = result.stdout.strip()
                return current_leader
        except Exception:
            continue
    
    current_leader = None
    return None

def print_cluster_status():
    """Print the current status of all nodes"""
    leader = detect_leader()
    
    print("\n==== CLUSTER STATUS ====")
    print(f"Current leader: {leader or 'Unknown'}")
    print("Nodes:")
    
    for node_id, process, status in processes:
        if status == "running":
            if node_id == leader:
                print(f"  - {node_id}: {status.upper()} (LEADER)")
            else:
                print(f"  - {node_id}: {status.upper()}")
        else:
            print(f"  - {node_id}: {status.upper()}")
    
    print("=======================")
    print("Commands:")
    print("  kill <node_id> - Kill a specific node. (node1, node2, etc are node_ids)", )
    print("  kill leader - Kill the current leader")
    print("  restart <node_id> - Restart a killed node")
    print("  status - Show cluster status")
    print("  quit - Exit the program")
    print("> ", end="", flush=True)

def kill_node(node_id):
    """Kill a specific node"""
    for i, (nid, process, status) in enumerate(processes):
        if nid == node_id and status == "running":
            print(f"Killing {node_id}...")
            try:
                process.terminate()
                process.wait(timeout=2)
                processes[i] = (nid, process, "manually killed")
                manually_killed.add(node_id)
                print(f"{node_id} killed successfully")
                return True
            except Exception as e:
                print(f"Error killing {node_id}: {e}")
                return False
    
    print(f"Node {node_id} is not running")
    return False

def restart_node(node_id, config):
    """Restart a manually killed node"""
    if node_id not in manually_killed:
        print(f"Node {node_id} was not manually killed")
        return False
        
    for i, (nid, old_process, status) in enumerate(processes):
        if nid == node_id and status == "manually killed":
            new_process = start_node(node_id, config)
            processes[i] = (nid, new_process, "running")
            manually_killed.remove(node_id)
            print(f"{node_id} restarted successfully")
            return True
    
    print(f"Could not find {node_id} in process list")
    return False

def monitor_processes(config):
    """Monitor processes and restart those that fail unexpectedly"""
    global running
    
    while running:
        time.sleep(1)
        
        # Check each process
        for i, (node_id, process, status) in enumerate(processes):
            # Skip manually killed processes
            if status == "manually killed":
                continue
                
            # Check if the process has terminated unexpectedly
            if process.poll() is not None and status == "running":
                print(f"\n{node_id} exited unexpectedly with code {process.poll()}")
                
                # Only restart if it wasn't manually killed
                if node_id not in manually_killed:
                    print(f"Automatically restarting {node_id}...")
                    new_process = start_node(node_id, config)
                    processes[i] = (node_id, new_process, "running")
                else:
                    processes[i] = (node_id, process, "manually killed")
                
                # Print status again
                print_cluster_status()

def command_processor():
    """Process user commands"""
    global running
    
    with open("nodes_config.json") as f:
        config = json.load(f)
    
    while running:
        cmd = input().strip()
        
        if cmd == "status":
            print_cluster_status()
        
        elif cmd == "quit" or cmd == "exit":
            running = False
            print("Shutting down...")
            break
            
        elif cmd == "kill leader":
            leader = detect_leader()
            if leader:
                kill_node(leader)
            else:
                print("Could not detect leader")
            print_cluster_status()
            
        elif cmd.startswith("kill "):
            node_id = cmd[5:].strip()
            if node_id in config:
                kill_node(node_id)
            else:
                print(f"Unknown node: {node_id}")
            print_cluster_status()
            
        elif cmd.startswith("restart "):
            node_id = cmd[8:].strip()
            if node_id in config:
                restart_node(node_id, config)
            else:
                print(f"Unknown node: {node_id}")
            print_cluster_status()
            
        else:
            print("Unknown command. Available commands: status, kill <node_id>, kill leader, restart <node_id>, quit")
            print("> ", end="", flush=True)

def main():
    global processes, running
    
    # Clear log files
    clear_log_files()
    
    # Load configuration
    with open("nodes_config.json") as f:
        config = json.load(f)
    
    try:
        # Start all nodes
        for node_id in config.keys():
            process = start_node(node_id, config)
            processes.append((node_id, process, "running"))
            time.sleep(1)  # Small delay between starting nodes
        
        print("\nAll nodes started successfully!")
        
        # Start monitoring thread
        monitor_thread = threading.Thread(target=monitor_processes, args=(config,))
        monitor_thread.daemon = True
        monitor_thread.start()
        
        # Print initial status
        time.sleep(2)  # Give nodes time to elect a leader
        print_cluster_status()
        
        # Process commands
        command_processor()
        
    except KeyboardInterrupt:
        print("\nInterrupted by user. Shutting down...")
    finally:
        # Cleanup
        running = False
        for node_id, process, status in processes:
            if status == "running":
                print(f"Terminating {node_id}...")
                try:
                    process.terminate()
                    process.wait(timeout=2)
                except Exception:
                    print(f"Forcefully killing {node_id}...")
                    process.kill()
        
        print("All nodes stopped")

if __name__ == "__main__":
    main()
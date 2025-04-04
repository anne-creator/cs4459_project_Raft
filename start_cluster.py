import subprocess
import json
import time
import sys
import os
import signal
import threading
import socket
import random

# Global variables
processes = []  # List of (node_id, process, status, port) tuples
manually_killed = set()  # Set of node_ids that were manually killed
current_leader = None
running = True

def clear_log_files(num_nodes):
    """Clear previous log files"""
    # First remove all existing node*.txt files
    for file in os.listdir():
        if file.startswith("node") and file.endswith(".txt"):
            try:
                os.remove(file)
                print(f"Removed previous file: {file}")
            except Exception as e:
                print(f"Warning: Could not remove {file}: {e}")
    
    # Also remove any state.json files
    for file in os.listdir():
        if file.startswith("node") and file.endswith("_state.json"):
            try:
                os.remove(file)
                print(f"Removed previous file: {file}")
            except Exception as e:
                print(f"Warning: Could not remove {file}: {e}")

def is_port_available(port):
    """Check if a port is available to use"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(('localhost', port))
            return True
        except socket.error:
            return False

def find_available_port(start_port=50051):
    """Find an available port starting from start_port"""
    port = start_port
    while not is_port_available(port):
        port += 1
        # Avoid searching indefinitely
        if port > start_port + 1000:
            raise RuntimeError("Could not find an available port in a reasonable range")
    return port

def generate_config(num_nodes):
    """Generate a dynamic configuration based on available ports"""
    config = {}
    ports = []
    
    # Start with default ports but find alternatives if they're taken
    start_port = 50051
    
    for i in range(1, num_nodes + 1):
        node_id = f"node{i}"
        port = find_available_port(start_port)
        ports.append(port)
        config[node_id] = {
            "address": f"localhost:{port}",
            "port": port
        }
        start_port = port + 1
    
    # Save the configuration to nodes_config.json
    with open("nodes_config.json", "w") as f:
        json.dump(config, f, indent=2)
    
    print(f"Created configuration with {num_nodes} nodes:")
    for node_id, info in config.items():
        print(f"  - {node_id}: port {info['port']}")
    
    return config

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
    return process, port

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
        for nid, proc, status, _ in processes:
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
    
    for node_id, process, status, port in processes:
        if status == "running":
            if node_id == leader:
                print(f"  - {node_id} (port {port}): {status.upper()} (LEADER)")
            else:
                print(f"  - {node_id} (port {port}): {status.upper()}")
        else:
            print(f"  - {node_id} (port {port}): {status.upper()}")
    
    print("=======================")
    print("Commands:")
    print("  kill <node_id> - Kill a specific node")
    print("  kill leader - Kill the current leader")
    print("  restart <node_id> - Restart a killed node")
    print("  status - Show cluster status")
    print("  quit - Exit the program")
    print("> ", end="", flush=True)

def kill_node(node_id):
    """Kill a specific node"""
    for i, (nid, process, status, port) in enumerate(processes):
        if nid == node_id and status == "running":
            print(f"Killing {node_id}...")
            try:
                process.terminate()
                process.wait(timeout=2)
                processes[i] = (nid, process, "manually killed", port)
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
        
    for i, (nid, old_process, status, port) in enumerate(processes):
        if nid == node_id and status == "manually killed":
            new_process, _ = start_node(node_id, config)
            processes[i] = (nid, new_process, "running", port)
            manually_killed.remove(node_id)
            print(f"{node_id} restarted successfully")
            return True
    
    print(f"Could not find {node_id} in process list")
    return False

# You can safely delete the following function as it's no longer needed
# def add_nodes(num_new_nodes, config):
#     """Add new nodes to the existing cluster"""
#     # This functionality has been removed
#     pass

def monitor_processes(config):
    """Monitor processes and restart those that fail unexpectedly"""
    global running, current_leader
    
    previous_leader = None
    
    while running:
        time.sleep(1)
        
        # Check for leader changes
        current_leader = detect_leader()
        if current_leader != previous_leader and current_leader is not None:
            print(f"\n==== NEW LEADER ELECTED: {current_leader} ====")
            print_cluster_status()
            previous_leader = current_leader
        
        # Check each process
        for i, (node_id, process, status, port) in enumerate(processes):
            # Skip manually killed processes
            if status == "manually killed":
                continue
                
            # Check if the process has terminated unexpectedly
            if process.poll() is not None and status == "running":
                print(f"\n{node_id} exited unexpectedly with code {process.poll()}")
                
                # Only restart if it wasn't manually killed
                if node_id not in manually_killed:
                    print(f"Automatically restarting {node_id}...")
                    new_process, _ = start_node(node_id, config)
                    processes[i] = (node_id, new_process, "running", port)
                else:
                    processes[i] = (node_id, process, "manually killed", port)
                
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
                print_cluster_status()
            else:
                print("Could not detect leader")
            print_cluster_status()
            
        elif cmd.startswith("kill "):
            node_id = cmd[5:].strip()
            if node_id in config:
                kill_node(node_id)
                print_cluster_status()
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
            
        elif cmd.startswith("add "):
            try:
                num_new_nodes = int(cmd[4:].strip())
                if num_new_nodes > 0:
                    config = add_nodes(num_new_nodes, config)
                    wait_for_leader()
                    print_cluster_status()
                else:
                    print("Number of nodes must be positive")
            except ValueError:
                print("Invalid number of nodes")
            
        else:
            print("Unknown command. Available commands: status, kill <node_id>, kill leader, restart <node_id>, add <num>, quit")
            print("> ", end="", flush=True)

def wait_for_leader(timeout=30):
    """Wait until a leader is elected or timeout expires"""
    start_time = time.time()
    print("Waiting for leader election...", end="", flush=True)
    
    while time.time() - start_time < timeout:
        leader = detect_leader()
        if leader:
            print(f"\nLeader elected: {leader}")
            return leader
        time.sleep(1)
        print(".", end="", flush=True)
    
    print("\nTimeout waiting for leader election!")
    return None

def main():
    global processes, running
    
    # Get number of nodes from command line or use default
    if len(sys.argv) > 1:
        try:
            num_nodes = int(sys.argv[1])
            if num_nodes < 3:
                print("Warning: RAFT requires at least 3 nodes for fault tolerance.")
                if num_nodes < 1:
                    print("Error: At least 1 node is required.")
                    return
        except ValueError:
            print("Error: Number of nodes must be an integer.")
            return
    else:
        num_nodes = 3
    
    # Clear log files
    clear_log_files(num_nodes)
    
    # Generate dynamic configuration
    config = generate_config(num_nodes)
    
    try:
        # Start all nodes
        for node_id in config.keys():
            process, port = start_node(node_id, config)
            processes.append((node_id, process, "running", port))
            time.sleep(1)  # Small delay between starting nodes
        
        print("\nAll nodes started successfully!")
        
        # Start monitoring thread
        monitor_thread = threading.Thread(target=monitor_processes, args=(config,))
        monitor_thread.daemon = True
        monitor_thread.start()
        
        # Wait for leader election before showing status
        wait_for_leader()
        
        # Print initial status
        print_cluster_status()
        
        # Process commands
        command_processor()
        
    except KeyboardInterrupt:
        print("\nInterrupted by user. Shutting down...")
    finally:
        # Cleanup
        running = False
        for node_id, process, status, _ in processes:
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
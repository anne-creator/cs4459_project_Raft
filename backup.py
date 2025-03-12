import os
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

import grpc
from concurrent import futures
import replication_pb2
import replication_pb2_grpc
import time
import threading
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc
from google.protobuf import empty_pb2

def send_heartbeats():
    """
    Send heartbeats to the heartbeat server every 5 seconds
    Uses exponential backoff for reconnection attempts
    """
    retry_delay = 5  # Initial retry delay (seconds)
    max_retry_delay = 30  # Maximum retry delay
    connected = False
    
    while True:
        try:
            # Try to establish connection to heartbeat server
            with grpc.insecure_channel('localhost:50053') as channel:
                stub = heartbeat_service_pb2_grpc.ViewServiceStub(channel)
                
                # If we get here, we've successfully connected
                if not connected:
                    print("Backup: Connected to heartbeat server")
                    connected = True
                    retry_delay = 5  # Reset retry delay
                
                while True:
                    try:
                        # Create heartbeat request with service identifier
                        request = heartbeat_service_pb2.HeartbeatRequest(service_identifier="backup")
                        
                        # Send heartbeat
                        stub.Heartbeat(request)
                        print("Backup: Sent heartbeat to heartbeat server")
                        
                        # Wait for 5 seconds before sending the next heartbeat
                        time.sleep(5)
                        
                    except grpc.RpcError as e:
                        print("Backup: Lost connection to heartbeat server, reconnecting...")
                        connected = False
                        break
        
        except Exception as e:
            connected = False
            print(f"Backup: Cannot connect to heartbeat server. Retrying in {retry_delay}s")
            time.sleep(retry_delay)
            # Use exponential backoff for retries
            retry_delay = min(retry_delay * 2, max_retry_delay)

class SequenceServicer(replication_pb2_grpc.SequenceServicer):
    def __init__(self):
        # Dictionary to store key-value pairs
        self.data = {}
        # Initialize log file
        with open("backup.txt", "w") as log_file:
            pass  # Create empty file or truncate existing
        
    def Write(self, request, context):
        """
        Handle Write requests from primary server
        """
        key = request.key
        value = request.value
        
        # Store data locally
        self.data[key] = value
        
        # Write the key-value pair to backup.txt
        with open('backup.txt', 'a') as f:
            f.write(f"{key} {value}\n")
        
        # Log that we received the request and wrote to file
        print(f"Backup: Received write request from primary: {key}:{value}")
        print(f"Backup: Successfully wrote {key}:{value} to backup.txt")
        
        # Return acknowledgment to primary using WriteResponse
        return replication_pb2.WriteResponse(ack="true")

def serve():
    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Add service to server
    service = SequenceServicer()
    replication_pb2_grpc.add_SequenceServicer_to_server(service, server)
    
    # Start server
    server.add_insecure_port('localhost:50052')
    server.start()
    
    print("Backup server started on port 50052")
    print("Waiting for connections from primary...")
    
    # Start the heartbeat thread
    heartbeat_thread = threading.Thread(target=send_heartbeats, daemon=True)
    heartbeat_thread.start()
    
    try:
        while True:
            time.sleep(86400)  # Sleep for a day
    except KeyboardInterrupt:
        server.stop(0)
        print("Backup server stopped")

if __name__ == '__main__':
    serve()
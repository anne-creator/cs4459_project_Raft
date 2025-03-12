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
                    print("Primary: Connected to heartbeat server")
                    connected = True
                    retry_delay = 5  # Reset retry delay
                
                while True:
                    try:
                        # Create heartbeat request with service identifier
                        request = heartbeat_service_pb2.HeartbeatRequest(service_identifier="primary")
                        
                        # Send heartbeat
                        stub.Heartbeat(request)
                        print("Primary: Sent heartbeat to heartbeat server")
                        
                        # Wait for 5 seconds before sending the next heartbeat
                        time.sleep(5)
                        
                    except grpc.RpcError as e:
                        print("Primary: Lost connection to heartbeat server, reconnecting...")
                        connected = False
                        break
        
        except Exception as e:
            connected = False
            print(f"Primary: Cannot connect to heartbeat server. Retrying in {retry_delay}s")
            time.sleep(retry_delay)
            # Use exponential backoff for retries
            retry_delay = min(retry_delay * 2, max_retry_delay)

class SequenceServicer(replication_pb2_grpc.SequenceServicer):
    def __init__(self):
        # Dictionary to store key-value pairs
        self.data = {}
        # Initialize log file
        with open("primary.txt", "w") as log_file:
            pass  # Create empty file or truncate existing
            
        # Add connection to backup server
        self.backup_channel = grpc.insecure_channel('localhost:50052')
        self.backup_stub = replication_pb2_grpc.SequenceStub(self.backup_channel)

    def Write(self, request, context):
        """
        Handle Write requests from clients
        """
        key = request.key
        value = request.value
        
        print(f"Primary: Received write request for {key}:{value}")
        
        # Forward the request to the backup server
        try:
            # Create write request for backup
            write_request = replication_pb2.WriteRequest(key=key, value=value)
            print(f"Primary: Forwarding {key}:{value} to backup...")
            
            # Send to backup
            backup_response = self.backup_stub.Write(write_request)
            print(f"Primary: Backup acknowledged: {backup_response.ack}")
            
            # Store data locally ONLY after receiving acknowledgment from backup
            self.data[key] = value
            
            # Log the write operation
            with open("primary.txt", "a") as log_file:
                log_file.write(f"{key} {value}\n")
                
            # Send acknowledgment to client
            return replication_pb2.WriteResponse(ack=f"Write successful for {key}:{value}")
            
        except grpc.RpcError as e:
            print(f"Primary: Error communicating with backup: {e.code()}: {e.details()}")
            # Return error to client since backup didn't acknowledge
            return replication_pb2.WriteResponse(ack=f"Failed: Backup server unavailable")

def serve():
    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Add service to server
    service = SequenceServicer()
    replication_pb2_grpc.add_SequenceServicer_to_server(service, server)
    
    # Start server
    server.add_insecure_port('localhost:50051')
    server.start()
    
    print("Primary server started on port 50051")
    print("Waiting for connections...")
    
    # Start the heartbeat thread
    heartbeat_thread = threading.Thread(target=send_heartbeats, daemon=True)
    heartbeat_thread.start()
    
    try:
        while True:
            time.sleep(86400)  # Sleep for a day
    except KeyboardInterrupt:
        server.stop(0)
        print("Primary server stopped")

if __name__ == '__main__':
    serve()
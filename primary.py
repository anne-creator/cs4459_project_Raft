import grpc
from concurrent import futures
import replication_pb2
import replication_pb2_grpc
import time

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
        
        # Forward the request to the backup server using Sequence method
        try:
            # Create write request for backup (using WriteRequest instead of SequenceRequest)
            write_request = replication_pb2.WriteRequest(key=key, value=value)
            print(f"Primary: Forwarding {key}:{value} to backup...")
            
            # Send to backup using Write method instead of Sequence
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
    
    try:
        while True:
            time.sleep(86400)  # Sleep for a day
    except KeyboardInterrupt:
        server.stop(0)
        print("Primary server stopped")


if __name__ == '__main__':
    serve()
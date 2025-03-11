import grpc
from concurrent import futures
import replication_pb2
import replication_pb2_grpc
import time

class SequenceServicer(replication_pb2_grpc.SequenceServicer):
    def __init__(self):
        # Dictionary to store key-value pairs (not used for this assignment)
        self.data = {}
        
    def Write(self, request, context):
        """
        Handle Write requests from primary server
        """
        key = request.key
        value = request.value
        
        # Write the key-value pair to backup.txt
        with open('backup.txt', 'a') as f:
            f.write(f"{key}:{value}\n")
        
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
    
    try:
        while True:
            time.sleep(86400)  # Sleep for a day
    except KeyboardInterrupt:
        server.stop(0)
        print("Backup server stopped")


if __name__ == '__main__':
    serve()
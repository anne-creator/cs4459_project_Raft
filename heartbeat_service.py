import os
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

import grpc
from concurrent import futures
import time
import datetime
import threading
from google.protobuf import empty_pb2
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc

class ViewServiceServicer(heartbeat_service_pb2_grpc.ViewServiceServicer):
    def __init__(self):
        # Store the last heartbeat time for each service
        self.last_heartbeat = {}
        self.service_status = {}  # Track if a service is up or down
        self.lock = threading.Lock()  # For thread safety
        
        # Initialize log file
        with open("heartbeat.txt", "w") as f:
            f.write(f"Heartbeat server started at {self.get_timestamp()}\n")
        
        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self.monitor_heartbeats)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
    
    def Heartbeat(self, request, context):
        """
        Process heartbeat from a service (primary or backup)
        """
        service_id = request.service_identifier
        
        with self.lock:
            # Record the current time as the last heartbeat
            current_time = time.time()
            self.last_heartbeat[service_id] = current_time
            
            # Always log the heartbeat
            self.log_heartbeat(f"{service_id.capitalize()} is alive. Latest heartbeat received at {self.get_timestamp()}")
            
            # Update the service status
            self.service_status[service_id] = True
        
        # Return an empty response as specified in the proto file
        return empty_pb2.Empty()
    
    def monitor_heartbeats(self):
        """
        Continuously monitor heartbeats and detect down services
        """
        timeout = 10  # Consider service down after 10 seconds without heartbeat
        
        while True:
            time.sleep(2)  # Check every 2 seconds
            current_time = time.time()
            
            with self.lock:
                for service_id, last_time in list(self.last_heartbeat.items()):
                    # Check if service hasn't sent a heartbeat for too long
                    if current_time - last_time > timeout:
                        # Only log if service status is changing from up to down
                        if service_id in self.service_status and self.service_status[service_id]:
                            self.service_status[service_id] = False
                            self.log_heartbeat(f"{service_id.capitalize()} might be down. Latest heartbeat received at {self.get_timestamp(last_time)}")
    
    def log_heartbeat(self, message):
        """
        Log heartbeat status to file
        """
        try:
            with open("heartbeat.txt", "a") as f:
                f.write(f"{message}\n")
            print(message)  # Also print to console for debugging
        except Exception as e:
            print(f"Error writing to log file: {e}")
    
    def get_timestamp(self, timestamp=None):
        """
        Get formatted timestamp string
        """
        if timestamp is None:
            timestamp = time.time()
        return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

def serve():
    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Add the service to the server
    servicer = ViewServiceServicer()
    heartbeat_service_pb2_grpc.add_ViewServiceServicer_to_server(servicer, server)
    
    # Start server
    server.add_insecure_port('localhost:50053')
    server.start()
    
    print(f"Heartbeat server started on port 50053 at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Waiting for heartbeats from primary and backup...")
    
    try:
        # Keep server running
        while True:
            time.sleep(86400)  # Sleep for a day
    except KeyboardInterrupt:
        server.stop(0)
        print("Heartbeat server stopped")

if __name__ == '__main__':
    serve()
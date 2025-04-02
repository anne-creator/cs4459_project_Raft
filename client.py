import grpc
import replication_pb2
import replication_pb2_grpc
import time

def write_key_value(stub, key, value):
    """
    Send a Write request to the primary server
    """
    try:
        # Create request
        request = replication_pb2.WriteRequest(key=key, value=value)
        
        # Send request to primary
        response = stub.Write(request)
        
        # Log the request to client.txt
        with open("client.txt", "a") as log_file:
            log_file.write(f"{key} {value}\n")
        
        print(f"Client: Write request for {key}:{value} - Response: {response.ack}")
        return True
    
    except grpc.RpcError as e:
        print(f"RPC Error: {e.code()}: {e.details()}")
        return False

def run():
    print("Connecting to primary server...")
    
    # Connect to primary server
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = replication_pb2_grpc.SequenceStub(channel)
        
        print("Connected to primary server!")
        
        # Wait for 3 seconds before sending data
        print("Waiting 3 seconds before sending data...")
        time.sleep(3)
        
        # Sample data to write to the server
        sample_data = [
            ("1", "book"),
            ("2", "laptop"),
            ("3", "coffee mug"),
            ("4", "headphones"),
            ("5", "backpack")
        ]
        
        # Write sample data automatically
        print("\nWriting sample data...")
        for key, value in sample_data:
            success = write_key_value(stub, key, value)
            if not success:
                print(f"Failed to write {key}:{value}")
        
        print("All sample data written. Exiting.")

if __name__ == '__main__':
    run()
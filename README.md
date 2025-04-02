# Running the System
### Step 1: Start the Heartbeat Server
python3 heartbeat_service.py

The heartbeat server will start on port 50053 and create a log file named heartbeat.txt.


### Step 2: Start the Backup Server
python3 backup.py

The backup server will start on port 50052 and begin sending heartbeats to the heartbeat server.


### Step 3: Start the Primary Server
python3 primary.py

The primary server will start on port 50051 and begin sending heartbeats to the heartbeat server.


### Step 4: Run the Client
python3 client.py

The client will connect to the primary server and send a series of write requests (with sample data included). Each request includes a key-value pair that will be written to both primary and backup servers.
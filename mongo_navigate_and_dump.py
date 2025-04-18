import pymongo
import subprocess
import os
from datetime import datetime

# MongoDB connection details
MONGO_URI = "mongodb://localhost:27017"
DUMP_DIR_BASE = "/mnt/sdcard/dump"

# Databases to process
TARGET_DBS = ["camera_frames", "t_camera_frames", "gps_frames"]

def ensure_dump_directory():
    """Ensure the base dump directory exists."""
    if not os.path.exists(DUMP_DIR_BASE):
        os.makedirs(DUMP_DIR_BASE)

def connect_to_mongo():
    """Connect to MongoDB and return the client."""
    try:
        client = pymongo.MongoClient(MONGO_URI)
        client.server_info()  # Test the connection
        return client
    except pymongo.errors.ConnectionError as e:
        print(f"Failed to connect to MongoDB: {e}")
        exit(1)

def get_latest_collection(db):
    """Get the name of the latest collection in a database."""
    collections = db.list_collection_names()
    
    # Filter out 'frames' collection for timestamp-based sorting
    timestamp_collections = [c for c in collections if c != 'frames']
    
    if not timestamp_collections:
        # If no timestamp collections, return 'frames' if it exists
        return 'frames' if 'frames' in collections else None
    
    # Sort collections by name (assuming timestamp format YYYYMMDD_HHMMSS)
    return sorted(timestamp_collections, reverse=True)[0]

def dump_collection(db_name, collection_name):
    """Dump the specified collection using mongodump."""
    try:
        # Create a timestamped directory for the dump
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dump_dir = os.path.join(DUMP_DIR_BASE, f"dump_{db_name}_{collection_name}")
        
        # Run mongodump for the specific collection
        cmd = [
            "mongodump",
            "--uri", MONGO_URI,
            "--db", db_name,
            "--collection", collection_name,
            "--out", dump_dir
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"Successfully dumped collection '{collection_name}' from database '{db_name}' to {dump_dir}")
            # Log the operation
            with open(os.path.join(DUMP_DIR_BASE, "dump.log"), "a") as log_file:
                log_file.write(f"Collection '{collection_name}' from database '{db_name}' dumped to {dump_dir} at {timestamp}\n")
            return True
        else:
            print(f"Failed to dump collection '{collection_name}' from database '{db_name}': {result.stderr}")
            return False
    except Exception as e:
        print(f"Error dumping collection '{collection_name}' from database '{db_name}': {e}")
        return False

def dump_last_db_entry():
    # Ensure dump directory exists
    ensure_dump_directory()

    # Connect to MongoDB
    client = connect_to_mongo()
    
    successful_dumps = []

    # Process target databases
    for db_name in TARGET_DBS:
        try:
            db = client[db_name]
            
            # Get the latest collection
            latest_collection = get_latest_collection(db)
            
            if latest_collection:
                # Dump the latest collection
                if dump_collection(db_name, latest_collection):
                    successful_dumps.append(f"{db_name}/{latest_collection}")
            else:
                print(f"No collections found in database '{db_name}'")
        except Exception as e:
            print(f"Error processing database '{db_name}': {e}")

    # Close the MongoDB connection
    client.close()
    
    # Final summary
    if successful_dumps:
        print(f"Task completed. Successfully dumped {len(successful_dumps)} collections: {', '.join(successful_dumps)}")
    else:
        print("Task completed. No collections were dumped.")

if __name__ == "__main__":
    dump_last_db_entry()
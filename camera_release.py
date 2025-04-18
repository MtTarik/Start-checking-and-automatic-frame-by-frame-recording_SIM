import os
import subprocess
import re

def release_all_cameras():
    """
    Function to release all USB camera resources that might be locked
    due to improper program termination.
    """
    try:
        # Get list of video devices
        video_devices = [f for f in os.listdir('/dev') if f.startswith('video')]
        
        # Find processes using these devices
        for device in video_devices:
            device_path = f'/dev/{device}'
            # Use fuser to find processes using this device
            result = subprocess.run(['fuser', device_path], 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE,
                                   text=True)
            
            # Extract PIDs from the output
            if result.stdout:
                pids = re.findall(r'\d+', result.stdout)
                print(f"Found processes using {device_path}: {pids}")
                
                # Kill these processes
                for pid in pids:
                    try:
                        os.kill(int(pid), 9)  # SIGKILL
                        print(f"Killed process {pid}")
                    except ProcessLookupError:
                        print(f"Process {pid} already terminated")
                    except Exception as e:
                        print(f"Error killing process {pid}: {e}")
        
        print("Camera resources cleanup completed")
        return True
    except Exception as e:
        print(f"Error during camera cleanup: {e}")
        return False

# Example usage
if __name__ == "__main__":
    release_all_cameras()
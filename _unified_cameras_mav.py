#!/usr/bin/env python3
from multiprocessing import Process, Queue, Value
from mongo_navigate_and_dump import dump_last_db_entry
from camera_release import release_all_cameras
import cv2
from datetime import datetime
import os
import time
import logging
import logging
from queue import Empty, Full
import os, sys
import time
from datetime import datetime
from multiprocessing import Process, Queue, Value
from picamera2 import Picamera2
from pymongo import MongoClient
try:
    from pymavlink import mavutil
    from pymavlink.dialects.v20 import ardupilotmega as mavlink
    PYMAVLINK_AVAILABLE = True
except ImportError:
    print("WARNING: pymavlink not installed. Telemetry functions will be simulated.")
    PYMAVLINK_AVAILABLE = False

import json
from multiprocessing import Process, Queue, Value  # Already present, ensure Queue is here
# import queue  # Add this import for queue.Full
LOCK_FILE = "/tmp/camera_capture.lock"
# Configuration (existing)
OUTPUT_DIR = "/mnt/sdcard/video"
WIDTH = 1640
HEIGHT = 1232
DEFAULT_FPS = 15
MONGO_URI = "mongodb://localhost:27017/"
THERMAL_DB_NAME = "t_camera_frames"  # For thermal camera database
RGB_DB_NAME = "camera_frames"  # For Picamera
GPS_DB_NAME = "gps_frames"  # For Picamera
LOG_FILE = "~/Works/flight_log.json"
MIN_ARM_DURATION = 0.5

# Add for thermal camera

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger('picamera2').setLevel(logging.WARNING)


class MAVCommProcess:
    def __init__(self, picam_queue, thermal_queue, db_queue, system_time_delta, status_queue):
        self.serial_port = '/dev/ttyUSB0'
        #self.serial_port = '/dev/ttyUSB0'
        self.baud_rate = 57600
        self.source_system = 1
        self.source_component = 191
        self.target_system = 1
        self.target_component = 1
        self.picam_queue = picam_queue
        self.thermal_queue = thermal_queue
        self.db_queue = db_queue
        self.status_queue = status_queue  # New queue for status messages
        self.conn = None
        self.system_time_delta = system_time_delta
        self.armed = False
        self.prev_armed = False
        self.mode = "UNKNOWN"
        self.last_armed_time = 0
        self.last_state_change_time = 0
        self.is_ready = False
        self.session_id = None
        self.previous_id = None

    def setup_mavlink(self):
        if not PYMAVLINK_AVAILABLE:

            # logger.debug("PyMAVLink not avaliable skipping NAVLink setup")
            logger.error("PyMAVLink not available")
            return False
        
        self.conn = mavutil.mavlink_connection(
            device=self.serial_port,
            baud=self.baud_rate,
            source_system=self.source_system,
            source_component=self.source_component,
            target_system=self.target_system,
            target_component=self.target_component
        )
        if self._wait_for_heartbeat():
            return True
        else:
            return False
    
    def _wait_for_heartbeat(self):
        """
        Wait for a heartbeat from the specified target system and component.
        
        Returns:
            bool: True if heartbeat received, False if timeout
        """
        timeout_counter = 0
        max_attempts = 10
        
        while timeout_counter < max_attempts:
            msg = self.conn.recv_match(type='HEARTBEAT', blocking=True, timeout=5)
            if msg and msg.get_srcSystem() in (0,1) and msg.get_srcComponent() in (0,1):
                print(f"Heartbeat received from system {msg.get_srcSystem()}, component {msg.get_srcComponent()}")
                return True
            else:
                print(f"Received heartbeat from unexpected system|component, continuing to wait...")
                timeout_counter += 1
                time.sleep(1)
        
        print(f"Failed to receive expected heartbeat after {max_attempts} attempts")
        #sys.exit(1)
        return False
    
    def send_statustext(self, severity, text):
        """
        Send a status text message to the flight controller.
        
        Args:
            severity: Message severity (0-7), where:
                0=Emergency, 1=Alert, 2=Critical, 3=Error, 4=Warning, 5=Notice, 6=Info, 7=Debug
            text: Message text
        """
        if not self.conn:
            logger.warning(f"Cannot send status text: MAVLink connection not established - {text}")
            return
            
        try:
            text = text[:50]  # MAVLink STATUSTEXT has 50 char limit
            self.conn.mav.statustext_send(
                severity,
                text.encode('utf-8')
            )
            logger.info(f"Sent status text: severity={severity}, text={text}")
        except Exception as e:
            logger.error(f"Error sending status text: {e}")

    def _sync_fc_time(self):
        if not self.conn:
            return False
        try:
            self.conn.mav.command_long_send(
                self.conn.target_system,
                self.conn.target_component,
                mavutil.mavlink.MAV_CMD_REQUEST_MESSAGE,
                0,
                mavutil.mavlink.MAVLINK_MSG_ID_SYSTEM_TIME,
                0, 0, 0, 0, 0, 0
            )
            start_time = time.time()
            while time.time() - start_time < 5.0:
                msg = self.conn.recv_match(type='SYSTEM_TIME', blocking=True, timeout=1.0)
                if msg:
                    fc_time_us = msg.time_unix_usec
                    local_time_us = int(time.time() * 1e6)
                    with self.system_time_delta.get_lock():
                        self.system_time_delta.value = fc_time_us - local_time_us
                    logger.info(f"Time sync established. Delta: {self.system_time_delta.value / 1e6:.6f} seconds")
                    return True
            logger.warning("Timed out waiting for SYSTEM_TIME")
            return False
        except Exception as e:
            logger.error(f"Error syncing time: {e}")
            return False

    def run(self):
        n = 0
        while  n < 5:
            if self.setup_mavlink():
                
                break
            else: 
                n += 1
                print(f"Mavlink fail #{n}")
                time.sleep(1)
        else:
            logger.error("Failed to setup MAVLink")
            return

            
        # while not self.conn.wait_heartbeat(timeout=10):
        #     logger.warning("Waiting for FC heartbeat...")
        
        if not self._sync_fc_time():
            logger.error("Failed to sync time with FC")
            return

        self.is_ready = True
        logger.info("MAVLink connection established and ready")
        self.send_statustext(0, f"Video recoder v 1.12 is active!")

        self.conn.mav.request_data_stream_send(
            self.conn.target_system,
            self.conn.target_component,
            mavutil.mavlink.MAV_DATA_STREAM_ALL,
            10, 1
        )
        self.conn.mav.request_data_stream_send(
            self.conn.target_system,
            self.conn.target_component,
            mavutil.mavlink.MAV_DATA_STREAM_POSITION,
            2, 1
        )
        
        last_hb_time = 0

        while True:
            # Check for status messages from other processes
            try:
                while not self.status_queue.empty():
                    status_msg = self.status_queue.get_nowait()
                    severity = status_msg['severity']
                    text = status_msg['text']
                    self.send_statustext(severity, text)
            except Empty:
                pass

            if time.time() - last_hb_time >= 1.0:
                self.conn.mav.heartbeat_send(
                    mavutil.mavlink.MAV_TYPE_GCS,
                    mavutil.mavlink.MAV_AUTOPILOT_INVALID,
                    0, 0, 0
                )
                last_hb_time = time.time()
            
            msg = self.conn.recv_match(blocking=True)
            if msg:
                if msg.get_type() == 'BAD_DATA':
                    msg = self.conn.recv_match(blocking=False)
                    continue
                
                msg_type = msg.get_type()
                fc_message = False
                if hasattr(msg, 'get_srcSystem') and hasattr(msg, 'get_srcComponent'):
                    src_system = msg.get_srcSystem()
                    src_component = msg.get_srcComponent()
                    fc_message = (src_system == 1 and src_component == 1)
                
                with self.system_time_delta.get_lock():
                    synced_time = (time.time() * 1e6 + self.system_time_delta.value) / 1e6
                
                if msg_type == 'HEARTBEAT' and fc_message:
                    self.prev_armed = self.armed
                    self.armed = (msg.base_mode & mavutil.mavlink.MAV_MODE_FLAG_SAFETY_ARMED) != 0
                    mode_mapping = mavutil.mode_mapping_bynumber(self.conn.mav_type)
                    if mode_mapping and msg.custom_mode in mode_mapping:
                        self.mode = mode_mapping[msg.custom_mode]
                    else:
                        self.mode = "UNKNOWN"
                    
                    if self.armed != self.prev_armed:
                        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S") if self.armed else None
                        logger.info(f"Vehicle {'ARMED' if self.armed else 'DISARMED'} in mode {self.mode} at {synced_time:.2f}")
                        self.last_armed_time = synced_time
                        self.last_state_change_time = synced_time
                        arm_msg = {
                            'armed': self.armed,
                            'timestamp': synced_time,
                            'flight_mode': self.mode,
                            'session_id': self.session_id
                        }
                        try:
                            self.picam_queue.put_nowait(arm_msg)
                            self.thermal_queue.put_nowait(arm_msg)
                            # if not self.armed:
                            self.prev_armed = self.armed
                        except Full:
                            logger.warning("Queue full, skipping arm/disarm event")
                
                elif msg_type == 'ATTITUDE' and self.armed:
                    attitude_msg = {
                        'roll': msg.roll,
                        'pitch': msg.pitch,
                        'yaw': msg.yaw,
                        'timestamp': synced_time
                    }
                    try:
                        self.picam_queue.put_nowait(attitude_msg)
                        self.thermal_queue.put_nowait(attitude_msg)
                    except Full:  # Fixed to queue.Full
                        logger.warning("Queue full, skipping attitude message")
                
                elif msg_type == 'RANGEFINDER' and self.armed:
                    rf_msg = {
                        'distance': msg.distance,
                        'timestamp': synced_time
                    }
                    try:
                        self.picam_queue.put_nowait(rf_msg)
                        self.thermal_queue.put_nowait(rf_msg)
                    except Full:  # Fixed to queue.Full
                        logger.warning("Queue full, skipping rangefinder message")
                
                elif msg_type == 'GPS_RAW_INT' and self.armed:
                    #print(msg)
                    pack = {
                        'type': 'gps',
                        'ts': synced_time,
                        'lat': msg.lat / 1e7,
                        'lon': msg.lon / 1e7,
                        'alt': msg.alt / 1000.0,
                        'fix_type': msg.fix_type,
                        'satellites': msg.satellites_visible
                    }
                    if self.session_id != self.previous_id:
                        pack['session_id'] = self.session_id
                        self.previous_id = self.session_id
                    self.db_queue.put_nowait(pack)
              

class ThermalCameraProcess:
    def __init__(self, thermal_queue, thermal_db_queue, system_time_delta, device_ids=None, status_queue=None):
        self.thermal_queue = thermal_queue
        self.thermal_db_queue = thermal_db_queue
        self.system_time_delta = system_time_delta
        self.status_queue = status_queue
        self.device_ids = device_ids if isinstance(device_ids, list) else [device_ids] if device_ids else [x for x in range(0, 25)]
        self.fps = DEFAULT_FPS
        self.camera = None
        self.current_device_id = None
        self.session_id = None
        self.is_running = False
        self.is_recording = False
        self.last_attitude = None
        self.last_range = None
        self.armed = False
        self.frame_number = 0
        self.reconnect_delay = 5
        self.last_reconnect_time = 0
        self.consecutive_failures = 0
        self.last_status_update = time.time()  # Initialize with current time
        self.status = "WAITING"

    def setup_camera(self):
        """Try to connect to one of the potential thermal cameras."""
        # Release any existing camera
        if self.camera is not None:
            self.camera.release()
            self.camera = None
            self.current_device_id = None
        
        # Try each device ID in the list
        for device_id in self.device_ids:
            try:
                logger.info(f"Trying to connect to thermal camera at index {device_id}")
                self.camera = cv2.VideoCapture(device_id)
                
                if not self.camera.isOpened():
                    logger.warning(f"Could not open thermal camera at index {device_id}")
                    continue
                
                # Try to read a test frame
                ret, frame = self.camera.read()
                if not ret or frame is None or frame.size == 0:
                    logger.warning(f"Could not read frame from thermal camera at index {device_id}")
                    self.camera.release()
                    self.camera = None
                    continue
                
                # Set camera properties
                self.camera.set(cv2.CAP_PROP_FPS, self.fps)
                self.camera.set(cv2.CAP_PROP_FRAME_WIDTH, WIDTH)
                self.camera.set(cv2.CAP_PROP_FRAME_HEIGHT, HEIGHT)
                
                # Get actual properties
                actual_fps = self.camera.get(cv2.CAP_PROP_FPS)
                width = self.camera.get(cv2.CAP_PROP_FRAME_WIDTH)
                height = self.camera.get(cv2.CAP_PROP_FRAME_HEIGHT)
                
                logger.info(f"Successfully connected to thermal camera at index {device_id}")
                logger.info(f"  Resolution: {width}x{height}, FPS: {actual_fps}")
                
                self.current_device_id = device_id
                self.consecutive_failures = 0
                return True
                
            except Exception as e:
                logger.error(f"Error connecting to thermal camera at index {device_id}: {e}")
        
        # If we get here, all connection attempts failed
        logger.error("Failed to connect to any thermal camera")
        return False

    def start_camera(self, session_id, reason=""):
        success = False
        try:
            if not self.is_running:
                if self.camera is None or not self.camera.isOpened():
                    if not self.setup_camera():
                        logger.error("Could not set up thermal camera")
                        self.status = "ERROR"
                        if self.status_queue:
                            self.status_queue.put_nowait({'severity': 3, 'text': "Thermal camera setup failed"})
                        return False
                self.is_running = True
                self.session_id = session_id
                self.frame_number = 0
                session_dir = os.path.join(os.path.expanduser(OUTPUT_DIR), self.session_id, "thermal")
                print("session_dir: ", session_dir)
                os.makedirs(session_dir, exist_ok=True)
                logger.info(f"Thermal camera started ({reason}) - Session {self.session_id}")
                self.log_event(f"Thermal camera started - Session {self.session_id}")
                self.status = "RECORDING"
                if self.status_queue:
                    self.status_queue.put_nowait({'severity': 4, 'text': "Thermal camera recording"})
                success = True
        except Exception as e:
            logger.error(f"Thermal camera start failed: {e}")
            self.status = "ERROR"
            if self.status_queue:
                self.status_queue.put_nowait({'severity': 3, 'text': f"Thermal camera error: {str(e)[:40]}"})
            sys.exit(1)
        return success

    def stop_camera(self, reason=""):
        try:
            if self.is_running:
                self.is_running = False
                if self.camera:
                    self.camera.release()
                    self.camera = None
                    self.current_device_id = None
                while not self.thermal_queue.empty():
                    try:
                        self.thermal_queue.get_nowait()
                    except Empty:
                        break
                logger.info(f"Thermal camera stopped ({reason}) - Session {self.session_id}")
                self.log_event(f"Thermal camera stopped - Session {self.session_id}")
                self.session_id = None
                self.status = "WAITING"
                if self.status_queue:
                    self.status_queue.put_nowait({'severity': 4, 'text': "Thermal camera waiting"})
        except Exception as e:
            logger.error(f"Thermal camera stop failed: {e}")
            self.status = "ERROR"
            if self.status_queue:
                self.status_queue.put_nowait({'severity': 3, 'text': f"Thermal stop error: {str(e)[:40]}"})

    def log_event(self, event):
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'event': event
        }
        with open(os.path.expanduser(LOG_FILE), 'a') as f:
            json.dump(log_entry, f)
            f.write('\n')

    def update_telemetry(self, current_time):
        armed_update = None
        while not self.thermal_queue.empty():
            try:
                data = self.thermal_queue.get_nowait()
                if 'armed' in data:
                    armed_update = data
                elif 'roll' in data:
                    self.last_attitude = data
                elif 'distance' in data:
                    self.last_range = data
            except Empty:
                break
        
        if self.last_attitude and (current_time - self.last_attitude['timestamp']) > 0.3:
            self.last_attitude = {
                'roll': 666,
                'pitch': 666,
                'yaw': 666,
                'timestamp': current_time
            }
        return armed_update

    def run(self):
        self.setup_camera()
        output_base = os.path.expanduser(OUTPUT_DIR)
        os.makedirs(output_base, exist_ok=True)
        
        frame_count = 0
        last_fps_time = time.time()
        frame_interval = 1.0 / self.fps
        last_periodic_time = time.time()  # Separate timer for periodic updates
        
        while True:
            try:
                loop_start = time.time()
                
                with self.system_time_delta.get_lock():
                    current_time = (time.time() * 1e6 + self.system_time_delta.value) / 1e6
                
                armed_update = self.update_telemetry(current_time)
                if armed_update:
                    new_armed_state = armed_update['armed']
                    session_id = armed_update.get('session_id')
                    if new_armed_state != self.armed:
                        if new_armed_state:
                            self.start_camera(session_id, f"Start - mode {armed_update['flight_mode']}")
                        else:
                            self.stop_camera("Stop")
                        self.armed = new_armed_state
                
                # Periodic status update (once a minute)
                if time.time() - last_periodic_time >= 60:
                    if self.status_queue:
                        self.status_queue.put_nowait({'severity': 3, 'text': f"Thermal status: {self.status}"})
                    last_periodic_time = time.time()  # Update the periodic timer
                
                if self.is_running:
                    if not self.camera or not self.camera.isOpened():
                        logger.warning("Thermal camera not available during recording")
                        self.consecutive_failures += 1
                        self.status = "ERROR"
                        if self.status_queue:
                            self.status_queue.put_nowait({'severity': 3, 'text': "Thermal camera unavailable"})
                        time.sleep(0.1)
                        continue
                        
                    self.frame_number += 1
                    filename = f"th_{self.frame_number:013d}.jpg"
                    session_dir = os.path.join(output_base, self.session_id, "thermal")
                    filepath = os.path.join(session_dir, filename)
                    
                    ret, frame = self.camera.read()
                    if not ret or frame is None or frame.size == 0:
                        logger.error("Failed to capture thermal frame")
                        self.consecutive_failures += 1
                        self.status = "ERROR"
                        if self.status_queue:
                            self.status_queue.put_nowait({'severity': 3, 'text': "Thermal capture failed"})
                        sys.exit(1)    
                        continue
                    
                    self.consecutive_failures = 0
                    cv2.imwrite(filepath, frame)
                    pack = {'type':'th', 'rf': self.last_range, 'att': self.last_attitude}
                    if self.frame_number == 1:
                        pack['session_id'] = session_id
                    self.thermal_db_queue.put_nowait(pack)
                
                elapsed = time.time() - loop_start
                sleep_time = max(0, frame_interval - elapsed)
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Thermal camera process error: {e}")
                self.status = "ERROR"
                if self.status_queue:
                    self.status_queue.put_nowait({'severity': 3, 'text': f"Thermal error: {str(e)[:40]}"})
                self.consecutive_failures += 1
                if self.is_running:
                    self.stop_camera("process error")
                sys.exit(1)
                time.sleep(1)

class CameraProcess:
    def __init__(self, picam_queue, rgb_db_queue, system_time_delta, status_queue=None):
        self.picam_queue = picam_queue
        self.rgb_db_queue = rgb_db_queue
        self.system_time_delta = system_time_delta
        self.status_queue = status_queue
        self.fps = DEFAULT_FPS
        self.camera = None
        self.session_id = None
        self.is_running = False
        self.last_attitude = None
        self.last_range = None
        self.armed = False
        self.frame_number = 0
        self.last_status_update = time.time()  # Initialize with current time
        self.status = "WAITING"

    def setup_camera(self):
        try:
            self.camera = Picamera2()
            config = self.camera.create_video_configuration(
                main={"size": (WIDTH, HEIGHT)},
                controls={"FrameRate": self.fps}
            )
            self.camera.configure(config)
            logger.info(f"Camera configured with FPS: {self.fps}")
            return True
        except Exception as e:
            logger.error(f"Camera setup failed: {e}")
            time.sleep(1)
            return False

    def start_camera(self, session_id, reason=""):
        try:
            if not self.is_running:
                self.camera.start()
                self.is_running = True
                self.session_id = session_id
                self.frame_number = 0
                session_dir = os.path.join(os.path.expanduser(OUTPUT_DIR), self.session_id, "rgb")
                print("session_dir: ", session_dir)
                os.makedirs(session_dir, exist_ok=True)
                logger.info(f"Camera started ({reason}) - Session {self.session_id}")
                self.log_event(f"Camera started - Session {self.session_id}")
                self.status = "RECORDING"
                if self.status_queue:
                    self.status_queue.put_nowait({'severity': 4, 'text': "RGB camera recording"})
        except Exception as e:
            logger.error(f"Camera start failed: {e}")
            self.status = "ERROR"
            if self.status_queue:
                self.status_queue.put_nowait({'severity': 3, 'text': f"RGB camera error: {str(e)[:40]}"})
            self.is_running = False
            sys.exit(1)

    def stop_camera(self, reason=""):
        try:
            if self.is_running:
                self.camera.stop()
                self.is_running = False
                logger.info(f"Camera stopped ({reason}) - Session {self.session_id}")
                self.log_event(f"Camera stopped - Session {self.session_id}")
                self.session_id = None
                self.status = "WAITING"
                if self.status_queue:
                    self.status_queue.put_nowait({'severity': 4, 'text': "RGB camera waiting"})
        except Exception as e:
            logger.error(f"Camera stop failed: {e}")
            self.status = "ERROR"
            if self.status_queue:
                self.status_queue.put_nowait({'severity': 3, 'text': f"RGB stop error: {str(e)[:40]}"})
            self.is_running = False

    def log_event(self, event):
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'event': event
        }
        with open(os.path.expanduser(LOG_FILE), 'a') as f:
            json.dump(log_entry, f)
            f.write('\n')

    def update_telemetry(self, current_time):
        armed_update = None
        while not self.picam_queue.empty():
            try:
                data = self.picam_queue.get_nowait()
                if 'armed' in data:
                    armed_update = data
                elif 'roll' in data:
                    self.last_attitude = data
                elif 'distance' in data:
                    self.last_range = data
            except Empty:
                break
        
        if self.last_attitude and (current_time - self.last_attitude['timestamp']) > 0.3:
            self.last_attitude = {
                'roll': 666,
                'pitch': 666,
                'yaw': 666,
                'timestamp': current_time
            }
        return armed_update

    def run(self):
        n = 0
        while  n < 5:
            if self.setup_camera():
                break
            else: 
                n += 1
                print(f"RGB camera fail #{n}")

        output_base = os.path.expanduser(OUTPUT_DIR)
        os.makedirs(output_base, exist_ok=True)
        
        frame_count = 0
        last_fps_time = time.time()
        frame_interval = 1.0 / self.fps
        last_periodic_time = time.time()  # Separate timer for periodic updates
        
        while True:
            try:
                loop_start = time.time()
                
                with self.system_time_delta.get_lock():
                    current_time = (time.time() * 1e6 + self.system_time_delta.value) / 1e6
                
                armed_update = self.update_telemetry(current_time)
                if armed_update:
                    new_armed_state = armed_update['armed']
                    session_id = armed_update.get('session_id')
                    if new_armed_state != self.armed:
                        if new_armed_state:
                            self.start_camera(session_id, f"Start - mode {armed_update['flight_mode']}")
                        else:
                            self.stop_camera("Stop")
                        self.armed = new_armed_state
                
                # Periodic status update (once a minute)
                if time.time() - last_periodic_time >= 60:
                    if self.status_queue:
                        self.status_queue.put_nowait({'severity': 4, 'text': f"RGB status: {self.status}"})
                    last_periodic_time = time.time()  # Update the periodic timer
                
                if self.is_running:
                    self.frame_number += 1
                    filename = f"pi_{self.frame_number:013d}.jpg"
                    session_dir = os.path.join(output_base, self.session_id, "rgb")
                    filepath = os.path.join(session_dir, filename)
                    
                    self.camera.capture_file(filepath, wait=False)
                    pack = {'type':'rgb', 'rf': self.last_range, 'att': self.last_attitude}
                    if self.frame_number == 1:
                        pack['session_id'] = session_id
                    self.rgb_db_queue.put_nowait(pack)
                
                elapsed = time.time() - loop_start
                sleep_time = max(0, frame_interval - elapsed)
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Camera process error: {e}")
                self.status = "ERROR"
                if self.status_queue:
                    self.status_queue.put_nowait({'severity': 3, 'text': f"RGB error: {str(e)[:40]}"})
                self.stop_camera("process error")
                sys.exit(1)
                time.sleep(1)

class DBProcess:
    def __init__(self, db_queue, rgb_db_queue, thermal_db_queue):
        self.rgb_db_queue = rgb_db_queue
        self.thermal_db_queue = thermal_db_queue
        self.db_queue = db_queue
        self.gps_current_session = None
        self.rgb_current_session = None
        self.thermal_current_session = None
        self.rgb_collection = None
        self.thermal_collection = None
        self.gps_collection = None
        self.done_sessions_set = set()

    def setup_mongodb(self, session_id, type):
        """Set up MongoDB connection and collection for the session."""
        if type == 'th':
            if self.thermal_current_session != session_id:
                self.client = MongoClient(MONGO_URI)
                self.db = self.client[THERMAL_DB_NAME]
                self.thermal_current_session = session_id
                self.thermal_collection = self.db[session_id]
                logger.info(f"Switched to thermal DB collection: {THERMAL_DB_NAME}.{session_id}")
        elif type == 'rgb':
            if self.rgb_current_session != session_id:
                self.client = MongoClient(MONGO_URI)
                self.db = self.client[RGB_DB_NAME]
                self.rgb_current_session = session_id
                self.rgb_collection = self.db[session_id]
                logger.info(f"Switched to DB collection: {RGB_DB_NAME}.{session_id}")
        elif type == 'gps':
            if self.gps_current_session != session_id:
                self.client = MongoClient(MONGO_URI)
                self.db = self.client[GPS_DB_NAME]
                self.gps_current_session = session_id
                self.gps_collection = self.db[session_id]
                logger.info(f"Switched to DB collection: {GPS_DB_NAME}.{session_id}")
        print("session_id", session_id)

    def run(self):
        """Process data from both queues and store in MongoDB."""
        wait_after_session = 3 # sec
        stop_session_timer = time.time()
        session = None 
        while True:
            if not self.rgb_db_queue.empty():
                data = self.rgb_db_queue.get()
                stop_session_timer = time.time()
                try:
                    if 'session_id' in data:
                        session = data.pop('session_id', None)
                        self.setup_mongodb(session, "rgb")
                        self.rgb_collection.insert_one(data)
                    else:
                        self.rgb_collection.insert_one(data)
                except Exception as e:
                    logger.error(f"RGB_DB Error: {e}")
            
            if not self.thermal_db_queue.empty():
                data = self.thermal_db_queue.get()
                stop_session_timer = time.time()
                try:
                    if 'session_id' in data:
                        session = data.pop('session_id', None)
                        self.setup_mongodb(session, "th")
                        self.thermal_collection.insert_one(data)
                    else:
                        self.thermal_collection.insert_one(data)
                except Exception as e:
                    logger.error(f"Thermal DB Error: {e}")

            if not self.db_queue.empty():
                data = self.db_queue.get()
                stop_session_timer = time.time()
                try:
                    if 'session_id' in data:
                        session = data.pop('session_id', None)
                        self.setup_mongodb(session, "gps")
                        self.gps_collection.insert_one(data)
                    else:
                        self.gps_collection.insert_one(data)
                        #stop_session_timer = time.time()
                except Exception as e:
                    logger.error(f"GPS Error: {e}")
            if  session and session not in self.done_sessions_set and  time.time() -  stop_session_timer >  wait_after_session:
                dump_last_db_entry()
                self.done_sessions_set.add(session)
            time.sleep(0.002)



def main():
    release_all_cameras()
    picam_queue = Queue(maxsize=20)
    thermal_queue = Queue(maxsize=20)
    db_queue = Queue(maxsize=20)
    thermal_db_queue = Queue(maxsize=10)
    rgb_db_queue = Queue(maxsize=10)
    status_queue = Queue(maxsize=50)
    system_time_delta = Value('d', 0.0)
    
    thermal_camera_indices = [*range(25)]
    
    mav_proc = MAVCommProcess(picam_queue, thermal_queue, db_queue, system_time_delta, status_queue)
    cam_proc = CameraProcess(picam_queue, rgb_db_queue, system_time_delta, status_queue=status_queue)
    db_proc = DBProcess(db_queue, rgb_db_queue, thermal_db_queue)
    thermal_proc = ThermalCameraProcess(thermal_queue, thermal_db_queue, system_time_delta, device_ids=thermal_camera_indices, status_queue=status_queue)
    
    processes = [
        Process(target=mav_proc.run),
        Process(target=cam_proc.run),
        Process(target=db_proc.run),
        Process(target=thermal_proc.run)
    ]
    
    # Set all processes as daemonic
    for p in processes:
        p.daemon = True  # Daemonic processes die with the parent
    
    # Start all processes
    for p in processes:
        p.start()
    
    # Monitor processes and kill main if one fails
    for p in processes:
        p.join()  # Wait for process to finish
        if p.exitcode != 0:  # Non-zero exit code means failure
            print(f"Process {p.name} failed with exit code {p.exitcode}")
            os._exit(1)  # Kill main process immediately (no cleanup)
    
    # If all processes complete successfully (unlikely with daemons), exit normally
    print("All processes completed successfully")
    sys.exit(0)

if __name__ == "__main__":
    # Lock file check
    if os.path.exists(LOCK_FILE):
        print(f"Another instance is already running! Lock file exists at {LOCK_FILE}")
        sys.exit(1)
    
    # Create the lock file
    try:
        with open(LOCK_FILE, 'w') as f:
            f.write(str(os.getpid()))  # Write the current process ID
    except Exception as e:
        print(f"Failed to create lock file {LOCK_FILE}: {e}")
        sys.exit(1)
    
    # Ensure lock file is removed on exit (even crashes)
    try:
        main()
    finally:
        try:
            os.remove(LOCK_FILE)
            print(f"Lock file {LOCK_FILE} removed")
        except Exception as e:
            print(f"Warning: Failed to remove lock file {LOCK_FILE}: {e}")
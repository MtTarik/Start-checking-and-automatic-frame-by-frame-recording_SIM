import cv2
import time
import os
import bson
from datetime import datetime
from pymavlink import mavutil

timestamp_root = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
SAVE_ROOT = os.path.join("captures", timestamp_root)

VIDEO_DIR = os.path.join(SAVE_ROOT, "VIDEO")
DAMP_DIR = os.path.join(SAVE_ROOT, "DAMP")

RGB_ID = "rgb_00"
IR_ID = "ir_01"

rgb_img_dir = os.path.join(VIDEO_DIR, RGB_ID)
ir_img_dir = os.path.join(VIDEO_DIR, IR_ID)
rgb_damp_dir = os.path.join(DAMP_DIR, RGB_ID)
ir_damp_dir = os.path.join(DAMP_DIR, IR_ID)

os.makedirs(rgb_img_dir, exist_ok=True)
os.makedirs(ir_img_dir, exist_ok=True)
os.makedirs(rgb_damp_dir, exist_ok=True)
os.makedirs(ir_damp_dir, exist_ok=True)

mav = mavutil.mavlink_connection('/dev/ttyTHS1', baud=57600)
mav.wait_heartbeat()

cam0 = cv2.VideoCapture(0)
cam1 = cv2.VideoCapture(1)

frame_id = 1001
print("▶ Реальний запис з Cube")

try:
    while True:
        t0 = time.time()
        ret0, frame0 = cam0.read()
        ret1, frame1 = cam1.read()
        t1 = time.time()

        if not ret0 or not ret1:
            print("⚠️ Кадр пропущено")
            continue

        rgb_filename = f"{RGB_ID}_{frame_id:013}.jpg"
        ir_filename = f"{IR_ID}_{frame_id:013}.jpg"

        cv2.imwrite(os.path.join(rgb_img_dir, rgb_filename), frame0)
        cv2.imwrite(os.path.join(ir_img_dir, ir_filename), frame1)

        att = mav.recv_match(type='ATTITUDE', blocking=True, timeout=1)
        if att:
            for cam_id, filename, damp_dir in [
                (RGB_ID, rgb_filename, rgb_damp_dir),
                (IR_ID, ir_filename, ir_damp_dir)
            ]:
                doc = {
                    "_id": filename,
                    "type": "th_2",
                    "rf": None,
                    "att": {
                        "roll": att.roll,
                        "pitch": att.pitch,
                        "yaw": att.yaw
                    },
                    "timestamp": round(t0, 8),
                    "frame_ts": round(t1, 8)
                }
                with open(os.path.join(damp_dir, filename.replace(".jpg", ".bson")), "wb") as f:
                    f.write(bson.BSON.encode(doc))

            print(f"💾 Кадр {frame_id} збережено")
            frame_id += 1

except KeyboardInterrupt:
    print("⏹ Запис завершено")

cam0.release()
cam1.release()

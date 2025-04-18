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

os.makedirs(VIDEO_DIR, exist_ok=True)
os.makedirs(DAMP_DIR, exist_ok=True)

# Визначення доступних камер
def detect_cameras(max_index=4):
    cameras = []
    for i in range(max_index):
        cap = cv2.VideoCapture(i)
        if cap.read()[0]:
            cameras.append(i)
        cap.release()
    return cameras

available_cams = detect_cameras()
print(f"📸 Доступні камери: {available_cams}")

cam0 = cv2.VideoCapture(available_cams[0]) if len(available_cams) > 0 else None
cam1 = cv2.VideoCapture(available_cams[1]) if len(available_cams) > 1 else None

ids = []
if cam0: ids.append("rgb_00")
if cam1: ids.append("ir_01")

paths = {}
for i, cam_id in enumerate(ids):
    vid_dir = os.path.join(VIDEO_DIR, cam_id)
    damp_dir = os.path.join(DAMP_DIR, cam_id)
    os.makedirs(vid_dir, exist_ok=True)
    os.makedirs(damp_dir, exist_ok=True)
    paths[cam_id] = {"video": vid_dir, "damp": damp_dir}

# MAVLink з’єднання
try:
    mav = mavutil.mavlink_connection('/dev/ttyTHS1', baud=57600)
    mav.wait_heartbeat()
except Exception as e:
    print(f"⚠️ Не вдалося підключитися до Cube: {e}")
    mav = None

frame_id = 1001
print("▶ Запуск з авто-визначенням камери")

try:
    while True:
        t0 = time.time()
        frame0 = cam0.read()[1] if cam0 else None
        frame1 = cam1.read()[1] if cam1 else None
        t1 = time.time()

        att_data = {
            "roll": 0.0,
            "pitch": 0.0,
            "yaw": 0.0
        }
        if mav:
            att = mav.recv_match(type='ATTITUDE', blocking=True, timeout=1)
            if att:
                att_data = {
                    "roll": att.roll,
                    "pitch": att.pitch,
                    "yaw": att.yaw
                }

        for cam_id, frame in zip(ids, [frame0, frame1]):
            if frame is None:
                continue

            filename = f"{cam_id}_{frame_id:013}.jpg"
            cv2.imwrite(os.path.join(paths[cam_id]["video"], filename), frame)

            doc = {
                "_id": filename,
                "type": "th_2",
                "rf": None,
                "att": att_data,
                "timestamp": round(t0, 8),
                "frame_ts": round(t1, 8)
            }

            bson_path = os.path.join(paths[cam_id]["damp"], filename.replace(".jpg", ".bson"))
            with open(bson_path, "wb") as f:
                f.write(bson.BSON.encode(doc))

        print(f"💾 Кадр {frame_id} збережено")
        frame_id += 1

except KeyboardInterrupt:
    print("⏹ Завершено")

if cam0: cam0.release()
if cam1: cam1.release()

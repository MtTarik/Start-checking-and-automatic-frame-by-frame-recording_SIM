import cv2
import time
import os
import bson
from pymavlink import mavutil

SAVE_DIR = "dual_output"
os.makedirs(os.path.join(SAVE_DIR, "cam_rgb"), exist_ok=True)
os.makedirs(os.path.join(SAVE_DIR, "cam_ir"), exist_ok=True)

bson_log = open(os.path.join(SAVE_DIR, "log.bson"), "wb")

# Підключення через USB-TTL до TELEM2
mav = mavutil.mavlink_connection('/dev/ttyUSB0', baud=57600)
mav.wait_heartbeat()

cam0 = cv2.VideoCapture(0)  # Raspberry Pi Cam
cam1 = cv2.VideoCapture(1)  # FLIR або інша IR Cam

frame_id = 1001
print("▶ Старт запису. Натисни Ctrl+C для зупинки.")

try:
    while True:
        t0 = time.time()
        ret0, frame0 = cam0.read()
        ret1, frame1 = cam1.read()
        t1 = time.time()

        if not ret0 or not ret1:
            print("⛔ Пропущено кадр")
            continue

        rgb_name = f"rgb_{frame_id:013}.jpg"
        ir_name = f"ir_{frame_id:013}.jpg"

        cv2.imwrite(os.path.join(SAVE_DIR, "cam_rgb", rgb_name), frame0)
        cv2.imwrite(os.path.join(SAVE_DIR, "cam_ir", ir_name), frame1)

        att = mav.recv_match(type='ATTITUDE', blocking=True, timeout=1)
        if att:
            doc = {
                "_id": f"pair_{frame_id}",
                "type": "dual",
                "timestamp": round(t0, 8),
                "frame_ts": round(t1, 8),
                "att": {
                    "roll": att.roll,
                    "pitch": att.pitch,
                    "yaw": att.yaw
                },
                "rgb_path": rgb_name,
                "ir_path": ir_name
            }
            bson_log.write(bson.BSON.encode(doc))
            print(f"💾 Кадр {frame_id} збережено")
            frame_id += 1

except KeyboardInterrupt:
    print("⏹ Запис завершено вручну.")

cam0.release()
cam1.release()
bson_log.close()


#bson_log.SAVE_DIR()


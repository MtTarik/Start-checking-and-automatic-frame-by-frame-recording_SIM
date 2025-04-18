import cv2
import time
import os
import bson
from datetime import datetime
import random
import numpy as np
FPS = int(input("Введіть бажану кількість кадрів в секунду (1–60): "))
if FPS < 1 or FPS > 60:
    print("❌ Некоректне значення FPS. Використовується 30 за замовчуванням.")
    FPS = 30

timestamp_root = datetime.now().strftime('%Y-%m-%d_%H-%M')
SAVE_ROOT = os.path.join("captures", timestamp_root)
VIDEO_DIR = os.path.join(SAVE_ROOT, "VIDEO")
DAMP_DIR = os.path.join(SAVE_ROOT, "DAMP")

os.makedirs(VIDEO_DIR, exist_ok=True)
os.makedirs(DAMP_DIR, exist_ok=True)

def detect_cameras(max_index=4):
    cameras = []
    for i in range(max_index):
        cap = cv2.VideoCapture(i, cv2.CAP_AVFOUNDATION)
        if cap.read()[0]:
            print(f"✅ Камера {i} активна")
            cameras.append(i)
        else:
            print(f"❌ Камера {i} недоступна")
        cap.release()
    return cameras

available_cams = detect_cameras()
print(f"📸 Доступні камери: {available_cams}")

if not available_cams:
    print("🚫 Немає доступних камер. Завершення.")
    exit()

cams = []
ids = []
for i, cam_index in enumerate(available_cams):
    cap = cv2.VideoCapture(cam_index, cv2.CAP_AVFOUNDATION)
    cap.set(cv2.CAP_PROP_FPS, FPS)
    cams.append(cap)
    ids.append(f"cam_{i:02}")

paths = {}
for cam_id in ids:
    vid_dir = os.path.join(VIDEO_DIR, cam_id)
    damp_dir = os.path.join(DAMP_DIR, cam_id)
    os.makedirs(vid_dir, exist_ok=True)
    os.makedirs(damp_dir, exist_ok=True)
    paths[cam_id] = {"video": vid_dir, "damp": damp_dir}

frame_id = 1001
print("▶ Тестовий запис на Mac (AVFoundation + bson.fix)")

recording = True
stopped = False

def draw_buttons(frame, recording):
    overlay = frame.copy()
    cv2.rectangle(overlay, (10, 10), (130, 50), (0, 255, 0) if recording else (0, 165, 255), -1)
    cv2.putText(overlay, "PAUSE" if recording else "START", (20, 40), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 0, 0), 2)

    cv2.rectangle(overlay, (150, 10), (260, 50), (0, 0, 255), -1)
    cv2.putText(overlay, "STOP", (170, 40), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)
    return overlay

def mouse_callback(event, x, y, flags, param):
    global recording, stopped
    if event == cv2.EVENT_LBUTTONDOWN:
        if 10 <= x <= 130 and 10 <= y <= 50:
            recording = not recording
        elif 150 <= x <= 260 and 10 <= y <= 50:
            stopped = True

cv2.namedWindow("Live Preview")
cv2.setMouseCallback("Live Preview", mouse_callback)

dumps_dict = {cam_id: [] for cam_id in ids}

try:
    while True:
        t0 = time.time()

        frames = []
        for cam in cams:
            ret, frame = cam.read()
            frames.append(frame if ret else None)

        t1 = time.time()


        att_data = {
            "roll": random.uniform(-0.05, 0.05),
            "pitch": random.uniform(-0.05, 0.05),
            "yaw": random.uniform(-3.14, 3.14)
        }


        for cam_id, frame in zip(ids, frames):
            if frame is None:
                continue

            if recording:
                filename = f"{cam_id}_{frame_id:013}.jpg"
                cv2.imwrite(os.path.join(paths[cam_id]["video"], filename), frame)

                doc = {
                    "_id": filename,
                    "type": "mac_test",
                    "rf": None,
                    "att": att_data,
                    "timestamp": round(t0, 8),
                    "frame_ts": round(t1, 8)
                }

                dumps_dict[cam_id].append(doc)

        print(f"💾 Кадр {frame_id} збережено")
        frame_id += 1

        if any([f is not None for f in frames]):
            resized_frames = [cv2.resize(f, (640, 480)) for f in frames if f is not None]
            preview = np.hstack(resized_frames)
            preview = draw_buttons(preview, recording)
            cv2.imshow("Live Preview", preview)
            if cv2.waitKey(1) & 0xFF == 27 or stopped:
                break

except KeyboardInterrupt:
    print("⏹ Тест завершено")

cv2.destroyAllWindows()
for cam in cams:
    cam.release()

for cam_id in ids:
    bson_path = os.path.join(paths[cam_id]["damp"], f"{cam_id}_dump.bson")
    with open(bson_path, "wb") as f:
        f.write(bson.encode({'frames': dumps_dict[cam_id]}))
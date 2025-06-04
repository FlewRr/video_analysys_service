import cv2
import base64


def extract_frames(video_path: str, max_frames: int = 10):
    frames = []
    cap = cv2.VideoCapture(video_path)
    success, frame = cap.read()
    count = 0
    while success and count < max_frames:
        # Convert BGR to RGB
        frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)

        # Encode RGB frame as JPEG bytes
        success_encode, jpeg = cv2.imencode('.jpg', frame_rgb)
        if not success_encode:
            print(f"Frame {count} encoding failed, skipping")
            success, frame = cap.read()
            continue

        # Convert JPEG bytes to base64 string
        jpg_base64 = base64.b64encode(jpeg.tobytes()).decode('utf-8')

        frames.append(jpg_base64)
        success, frame = cap.read()
        count += 1

    cap.release()
    return frames

import cv2
import requests
import logging
logging.basicConfig(level=logging.INFO)

def send_prediction_to_api(scenario_id, caption, extra="batch"):
    if caption:
        try:
            response = requests.post(
                f"http://localhost:8000/prediction/{scenario_id}/",
                json={"label": caption, "confidence": "1.0", "extra": extra},
                timeout=10
            )
            response.raise_for_status()
            logging.info(f"[{scenario_id}] Prediction saved to API.")
        except requests.RequestException as e:
            logging.error(f"[{scenario_id}] Failed to send prediction to API: {e}")

def process_video(video_path, scenario_id, frame_stride=30, batch_size=5):
    logging.info(f"[{scenario_id}] Video processing has started: {video_path}")
    cap = cv2.VideoCapture(video_path)
    frame_count = 0
    batch_images = []

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        if frame_count % frame_stride == 0:
            resized_frame = cv2.resize(frame, (224, 224))
            _, img_encoded = cv2.imencode('.jpg', resized_frame)
            batch_images.append(
                ("files", ("frame.jpg", img_encoded.tobytes(), "image/jpeg"))
            )

            if len(batch_images) == batch_size:
                try:
                    response = requests.post(
                        f"http://localhost:8003/inference/{scenario_id}/",
                        files=batch_images,
                        timeout=100
                    )

                    caption = response.json().get("prediction")
                    logging.info(f"[{scenario_id}] Batch prediction: {response.json()}")
                    send_prediction_to_api(scenario_id, caption)

                except requests.RequestException as e:
                    logging.error(f"[{scenario_id}] Failed to send batch: {e}")
                batch_images = []

        frame_count += 1

    if batch_images:
        try:
            response = requests.post(
                f"http://localhost:8003/inference/{scenario_id}/",
                files=batch_images,
                timeout=100
            )
            caption = response.json().get("prediction")
            logging.info(f"[{scenario_id}] Final batch prediction: {response.json()}")
            send_prediction_to_api(scenario_id, caption)

        except requests.RequestException as e:
            logging.error(f"[{scenario_id}] Failed to send final batch to inference: {e}")

    logging.info(f"[{scenario_id}] Video processing is finished.")

    cap.release()


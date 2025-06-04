import json
import cv2
import numpy as np
from kafka import KafkaConsumer
from inference.config import KAFKA_BOOTSTRAP_SERVERS, INFERENCE_TOPIC
from inference.yolo_nano import YoloNano
from inference.kafka_producer import send_prediction

consumer = KafkaConsumer(
    INFERENCE_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id="inference-group"
)

yolo = YoloNano(device='cpu')  # or 'cuda' if GPU available

def decode_frame(frame_data):
    # frame_data expected as base64 encoded or list of ints
    # For simplicity, assume runner sends raw bytes as list of ints for now
    np_frame = np.array(frame_data, dtype=np.uint8)
    frame = cv2.imdecode(np_frame, cv2.IMREAD_COLOR)
    return frame

def listen():
    print("[Inference] Listening for frames...")
    for msg in consumer:
        message = msg.value
        scenario_id = message.get("scenario_id")
        frame_data = message.get("frame")  # e.g. list of bytes or base64 str

        if scenario_id is None or frame_data is None:
            print("[Inference] Invalid message format")
            continue

        # Decode frame (implement your decoding here)
        frame = decode_frame(frame_data)

        if frame is None:
            print("[Inference] Failed to decode frame")
            continue

        predictions = yolo.predict(frame)
        send_prediction(scenario_id, predictions)

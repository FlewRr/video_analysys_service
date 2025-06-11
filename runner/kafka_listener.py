import json
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
from config import KAFKA_BOOTSTRAP_SERVERS, RUNNER_TOPIC, PREDICTION_TOPIC
from video_utils import extract_frames
from inference_client import send_to_inference
import os

for i in range(10):
    try:
        consumer = KafkaConsumer(
            RUNNER_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id="runner-group"
        )

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        break
    except NoBrokersAvailable:
        print("Kafka not available yet, retrying...") 
        time.sleep(3)
else:
    raise Exception("Could not connect to Kafka after retrying")


def handle_start(message):
    scenario_id = message["scenario_id"]
    video_path = message["video_path"]
    print(f"[Runner] Starting scenario {scenario_id}")
    
    if not os.path.exists(video_path):
        print(f"[Runner] Video not found: {video_path}")
        return

    frames = extract_frames(video_path)
    print(f"[Runner] Extracted {len(frames)} frames")

    predictions = send_to_inference(scenario_id, frames)
    print(f"[Runner] Got predictions")

    producer.send(PREDICTION_TOPIC, {
        "scenario_id": scenario_id,
        "predictions": predictions
    })
    producer.flush()
    print(f"[Runner] Sent predictions to orchestrator")

def listen():
    print("[Runner] Listening for Kafka messages...")
    for msg in consumer:
        message = msg.value
        msg_type = message.get("type")
        scenario_id = message.get("scenario_id")

        if msg_type == "start":
            handle_start(message)
        elif msg_type == "state_change":
            print(f"[Runner] Scenario {scenario_id} changed state to {message['new_state']}")
        else:
            print(f"[Runner] Unknown message type: {msg_type}")

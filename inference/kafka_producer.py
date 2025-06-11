import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from config import KAFKA_BOOTSTRAP_SERVERS, PREDICTION_TOPIC

producer = None

def get_producer():
    global producer
    if producer is not None:
        return producer

    for i in range(10):
        try:
            print(f"[Producer] Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} (attempt {i+1})")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("[Producer] Connected to Kafka")
            break
        except NoBrokersAvailable:
            print("[Producer] Kafka not available yet, retrying...")
            time.sleep(3)
    else:
        raise Exception("Could not connect to Kafka after retrying")

    return producer

def send_prediction(scenario_id, predictions):
    prod = get_producer()
    message = {
        "scenario_id": scenario_id,
        "predictions": predictions
    }
    prod.send(PREDICTION_TOPIC, message)
    prod.flush()

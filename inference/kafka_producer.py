import json
from kafka import KafkaProducer
from inference.config import KAFKA_BOOTSTRAP_SERVERS, PREDICTION_TOPIC

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_prediction(scenario_id, predictions):
    message = {
        "scenario_id": scenario_id,
        "predictions": predictions
    }
    producer.send(PREDICTION_TOPIC, message)
    producer.flush()

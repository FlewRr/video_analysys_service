import json
from kafka import KafkaProducer
from api.config import KAFKA_BOOTSTRAP_SERVERS, SCENARIO_TOPIC

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_scenario_message(message: dict):
    producer.send(SCENARIO_TOPIC, message)
    producer.flush()

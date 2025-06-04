import json
from kafka import KafkaProducer
from orchestrator.config import KAFKA_BOOTSTRAP_SERVERS, RUNNER_TOPIC

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_runner_command(message: dict):
    producer.send(RUNNER_TOPIC, message)
    producer.flush()

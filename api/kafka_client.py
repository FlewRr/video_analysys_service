import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from config import KAFKA_BOOTSTRAP_SERVERS, SCENARIO_TOPIC

for i in range(10):
    try:
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

def send_scenario_message(message: dict):
    producer.send(SCENARIO_TOPIC, message)
    producer.flush()

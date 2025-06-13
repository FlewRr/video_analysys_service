import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
import time
import logging
import os

logger = logging.getLogger(__name__)

class OrchestratorKafkaProducer:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(OrchestratorKafkaProducer, cls).__new__(cls)
            cls._instance.producer = None
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self._connect_producer()

    def _connect_producer(self):
        max_retries = 30  # Increased retries for Docker environment
        retry_delay = 5   # Increased delay between retries
        
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',  # Wait for all replicas to acknowledge
                    retries=3,   # Number of retries for failed requests
                    max_in_flight_requests_per_connection=1  # Ensure ordering
                )
                logger.info("[KafkaProducer] Successfully connected to Kafka")
                return
            except (NoBrokersAvailable, KafkaError) as e:
                logger.warning(f"[KafkaProducer] Attempt {attempt + 1}/{max_retries}: Kafka not available yet: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Could not connect to Kafka after {max_retries} attempts")

    def send_runner_command(self, command_data):
        try:
            self.producer.send(os.getenv('RUNNER_TOPIC'), value=command_data)
            self.producer.flush()
            logger.info("[KafkaProducer] Successfully sent runner command")
        except Exception as e:
            logger.error(f"[KafkaProducer] Error sending runner command: {str(e)}")
            raise

    def close(self):
        if self.producer:
            self.producer.close()
            logger.info("[KafkaProducer] Producer closed")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

# Export the send_runner_command function
def send_runner_command(message: dict):
    return OrchestratorKafkaProducer.get_instance().send_runner_command(message)

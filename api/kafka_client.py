import json
import time
import logging
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

logger = logging.getLogger(__name__)

class KafkaClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(KafkaClient, cls).__new__(cls)
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
                logger.info("[KafkaClient] Successfully connected to Kafka")
                return
            except (NoBrokersAvailable, KafkaError) as e:
                logger.warning(f"[KafkaClient] Attempt {attempt + 1}/{max_retries}: Kafka not available yet: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Could not connect to Kafka after {max_retries} attempts")

    def send_scenario_message(self, message):
        try:
            self.producer.send(os.getenv('SCENARIO_TOPIC'), value=message)
            self.producer.flush()
            logger.info("[KafkaClient] Successfully sent scenario message")
        except Exception as e:
            logger.error(f"[KafkaClient] Error sending scenario message: {str(e)}")
            raise

    def close(self):
        if self.producer:
            self.producer.close()
            logger.info("[KafkaClient] Producer closed")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

# Export the send_scenario_message function
def send_scenario_message(message: dict):
    return KafkaClient.get_instance().send_scenario_message(message)

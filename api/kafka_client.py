import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
from config import KAFKA_BOOTSTRAP_SERVERS, SCENARIO_TOPIC

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
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
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

    def send_scenario_message(self, message: dict):
        try:
            if not self.producer:
                logger.error("[KafkaClient] Producer not initialized")
                self._connect_producer()
            
            future = self.producer.send(SCENARIO_TOPIC, message)
            # Wait for the message to be delivered
            future.get(timeout=10)
            logger.info(f"[KafkaClient] Successfully sent message to {SCENARIO_TOPIC}")
        except Exception as e:
            logger.error(f"[KafkaClient] Error sending message: {str(e)}")
            raise

    def close(self):
        if self.producer:
            try:
                self.producer.flush()
                self.producer.close()
                self.producer = None
                logger.info("[KafkaClient] Successfully closed producer")
            except Exception as e:
                logger.error(f"[KafkaClient] Error closing producer: {str(e)}")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

# Export the send_scenario_message function
def send_scenario_message(message: dict):
    return KafkaClient.get_instance().send_scenario_message(message)

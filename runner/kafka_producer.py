import json
import time
import logging
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

logger = logging.getLogger(__name__)

class RunnerKafkaProducer:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RunnerKafkaProducer, cls).__new__(cls)
            cls._instance.producer = None
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self._connect_producer()

    def _connect_producer(self):
        max_retries = 30
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"[Producer] Connecting to Kafka at {os.getenv('KAFKA_BOOTSTRAP_SERVERS')} (attempt {attempt + 1}/{max_retries})")
                self.producer = KafkaProducer(
                    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3
                )
                logger.info("[Producer] Successfully connected to Kafka")
                return
            except (NoBrokersAvailable, KafkaError) as e:
                logger.warning(f"[Producer] Kafka not available yet: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Could not connect to Kafka after {max_retries} attempts")

    def send_scenario_message(self, message: dict):
        """Send message to scenario topic"""
        try:
            if not self.producer:
                logger.error("[Producer] Producer not initialized")
                self._connect_producer()
            
            # Use SCENARIO_TOPIC for shutdown messages, RUNNER_TOPIC for other messages
            topic = os.getenv('SCENARIO_TOPIC')
            if not topic:
                raise ValueError(f"Topic environment variable is not set for message type: {message.get('type')}")
            
            future = self.producer.send(topic, message)
            future.get(timeout=10)  # Wait for the message to be delivered
            logger.info(f"[Producer] Successfully sent scenario message to {topic}: {message}")
        except Exception as e:
            logger.error(f"[Producer] Error sending scenario message: {str(e)}")
            raise

    def close(self):
        if self.producer:
            try:
                self.producer.flush()
                self.producer.close()
                self.producer = None
                logger.info("[Producer] Successfully closed producer")
            except Exception as e:
                logger.error(f"[Producer] Error closing producer: {str(e)}")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

# Export the send_scenario_message function
def send_scenario_message(message: dict):
    return RunnerKafkaProducer.get_instance().send_scenario_message(message) 
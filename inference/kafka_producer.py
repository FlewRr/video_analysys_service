import json
import time
import logging
from kafka import KafkaProducer as KafkaClient
from kafka.errors import NoBrokersAvailable, KafkaError
from config import KAFKA_BOOTSTRAP_SERVERS, PREDICTION_TOPIC

logger = logging.getLogger(__name__)

class InferenceKafkaProducer:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(InferenceKafkaProducer, cls).__new__(cls)
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
                logger.info(f"[Producer] Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} (attempt {attempt + 1}/{max_retries})")
                self.producer = KafkaClient(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
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

    def send_prediction(self, scenario_id: str, predictions: dict):
        """Send predictions to Kafka"""
        try:
            if not self.producer:
                logger.error("[Producer] Producer not initialized")
                self._connect_producer()
            
            message = {
                "scenario_id": scenario_id,
                "predictions": predictions
            }
            future = self.producer.send(PREDICTION_TOPIC, message)
            future.get(timeout=10)  # Wait for the message to be delivered
            logger.info(f"[Producer] Successfully sent prediction for scenario {scenario_id}")
        except Exception as e:
            logger.error(f"[Producer] Error sending prediction: {str(e)}")
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

# Export the send_prediction function for backward compatibility
def send_prediction(scenario_id: str, predictions: dict):
    return InferenceKafkaProducer.get_instance().send_prediction(scenario_id, predictions)

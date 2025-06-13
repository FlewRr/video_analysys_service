import json
import time
import logging
import os
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
from inference_client import InferenceClient

logger = logging.getLogger(__name__)

class KafkaListener:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(KafkaListener, cls).__new__(cls)
            cls._instance.consumer = None
            cls._instance.inference_client = None
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.running = True
            self._connect_consumer()
            self.inference_client = InferenceClient.get_instance()

    def _connect_consumer(self):
        max_retries = 30  # Increased retries for Docker environment
        retry_delay = 5   # Increased delay between retries
        
        for attempt in range(max_retries):
            try:
                self.consumer = KafkaConsumer(
                    os.getenv('RUNNER_TOPIC'),
                    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    group_id="runner-group",
                    auto_offset_reset='earliest',
                    enable_auto_commit=True
                )
                logger.info("[KafkaListener] Successfully connected to Kafka")
                return
            except (NoBrokersAvailable, KafkaError) as e:
                logger.warning(f"[KafkaListener] Attempt {attempt + 1}/{max_retries}: Kafka not available yet: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Could not connect to Kafka after {max_retries} attempts")

    def handle_message(self, message: dict):
        try:
            msg_type = message.get("type")
            scenario_id = message.get("scenario_id")

            if not scenario_id:
                logger.error("[Runner] Received message without scenario_id")
                return

            if msg_type == "start":
                video_path = message.get("video_path")
                if not video_path:
                    logger.error("[Runner] Received start message without video_path")
                    return

                logger.info(f"[Runner] Starting processing for video: {video_path}")
                try:
                    self.inference_client.send_to_inference(scenario_id, video_path)
                    logger.info(f"[Runner] Successfully sent video for inference: {video_path}")
                except Exception as e:
                    logger.error(f"[Runner] Error processing video: {str(e)}")
                    raise

            elif msg_type == "shutdown":
                logger.info(f"[Runner] Received shutdown command for scenario: {scenario_id}")
                # Add any cleanup logic here if needed

        except Exception as e:
            logger.error(f"[Runner] Error handling message: {str(e)}")
            raise

    def listen(self):
        logger.info("[KafkaListener] Starting to listen for Kafka messages...")
        while self.running:
            try:
                for msg in self.consumer:
                    if not self.running:
                        break
                    message = msg.value
                    self.handle_message(message)
            except Exception as e:
                logger.error(f"[KafkaListener] Error in message processing loop: {str(e)}")
                if self.running:
                    logger.info("[KafkaListener] Attempting to reconnect...")
                    time.sleep(5)
                    self._connect_consumer()

    def stop(self):
        self.running = False
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("[KafkaListener] Successfully closed consumer")
            except Exception as e:
                logger.error(f"[KafkaListener] Error closing consumer: {str(e)}")
        if self.inference_client:
            self.inference_client.close()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

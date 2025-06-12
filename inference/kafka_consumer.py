import json
import cv2
import numpy as np
import time
import logging
import base64
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
from config import KAFKA_BOOTSTRAP_SERVERS, INFERENCE_TOPIC
from yolo import YoloNano
from kafka_producer import send_prediction

logger = logging.getLogger(__name__)

class InferenceKafkaConsumer:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(InferenceKafkaConsumer, cls).__new__(cls)
            cls._instance.consumer = None
            cls._instance.yolo = None
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.running = True
            logger.info("[KafkaConsumer] Initializing inference consumer...")
            self._connect_consumer()
            self._initialize_yolo()

    def _initialize_yolo(self):
        try:
            logger.info("[KafkaConsumer] Initializing YOLO model...")
            self.yolo = YoloNano(device='cpu')  # or 'cuda' if GPU available
            logger.info("[KafkaConsumer] Successfully initialized YOLO model")
        except Exception as e:
            logger.error(f"[KafkaConsumer] Error initializing YOLO model: {str(e)}")
            raise

    def _connect_consumer(self):
        max_retries = 30
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"[KafkaConsumer] Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})")
                self.consumer = KafkaConsumer(
                    INFERENCE_TOPIC,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    group_id="inference-group",
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000
                )
                logger.info("[KafkaConsumer] Successfully connected to Kafka")
                return
            except (NoBrokersAvailable, KafkaError) as e:
                logger.warning(f"[KafkaConsumer] Attempt {attempt + 1}/{max_retries}: Kafka not available yet: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Could not connect to Kafka after {max_retries} attempts")

    def decompress_frame(self, compressed_frame: str, shape: tuple) -> np.ndarray:
        """Decompress frame data from base64 string"""
        try:
            frame_bytes = base64.b64decode(compressed_frame)
            frame = cv2.imdecode(np.frombuffer(frame_bytes, np.uint8), cv2.IMREAD_COLOR)
            if frame is None:
                raise ValueError("Failed to decode frame from JPEG bytes")

            frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
         
            logger.info(f"[Inference] Successfully decompressed frame with shape {frame.shape}")
            return frame
        except Exception as e:
            logger.error(f"[Inference] Error decompressing frame: {str(e)}")
            raise

    def handle_message(self, message: dict):
        try:
            scenario_id = message.get("scenario_id")
            frame_index = message.get("frame_index")
            compressed_frame = message.get("frame")
            frame_shape = message.get("frame_shape")

            if not all([scenario_id, frame_index is not None, compressed_frame, frame_shape]):
                logger.error(f"[Inference] Received invalid message format: {message}")
                return

            logger.info(f"[Inference] Processing frame {frame_index} for scenario: {scenario_id}")
            try:
                frame = self.decompress_frame(compressed_frame, frame_shape)
                
                predictions = self.yolo.predict(frame)
                logger.info(f"[Inference] YOLO found {len(predictions)} objects in frame {frame_index}")
                
                logger.info(f"[Inference] Sending prediction for frame {frame_index}")
                send_prediction(scenario_id, {
                    "frame_index": frame_index,
                    "predictions": predictions
                })
                logger.info(f"[Inference] Successfully processed and sent prediction for frame {frame_index}")

            except Exception as e:
                logger.error(f"[Inference] Error processing frame: {str(e)}")
                raise

        except Exception as e:
            logger.error(f"[Inference] Error handling message: {str(e)}")
            raise

    def listen(self):
        logger.info("[KafkaConsumer] Starting to listen for Kafka messages...")
        while self.running:
            try:
                for msg in self.consumer:
                    if not self.running:
                        break
                    if msg.topic == INFERENCE_TOPIC:
                        logger.info(f"[KafkaConsumer] Received message for frame {msg.value.get('frame_index')}")
                        self.handle_message(msg.value)
                    else:
                        logger.warning(f"[KafkaConsumer] Received message from unexpected topic: {msg.topic}")
            except Exception as e:
                logger.error(f"[KafkaConsumer] Error in message processing loop: {str(e)}")
                if self.running:
                    logger.info("[KafkaConsumer] Attempting to reconnect...")
                    time.sleep(5)
                    self._connect_consumer()

    def stop(self):
        self.running = False
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("[KafkaConsumer] Successfully closed consumer")
            except Exception as e:
                logger.error(f"[KafkaConsumer] Error closing consumer: {str(e)}")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

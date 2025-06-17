import json
import cv2
import numpy as np
import time
import logging
import os
import base64
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
from yolo import YoloNano
from kafka_producer import send_prediction
from heartbeat import HeartbeatSender

logger = logging.getLogger(__name__)

class InferenceKafkaConsumer:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(InferenceKafkaConsumer, cls).__new__(cls)
            cls._instance.consumer = None
            cls._instance.yolo = None
            cls._instance.heartbeat_senders = {}  # Track heartbeat senders per scenario
            
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.running = True
            logger.info("[KafkaConsumer] Initializing inference consumer...")
            self._connect_consumer()
            self._initialize_yolo()
            self.active_scenarios = set()

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
                    os.getenv('INFERENCE_TOPIC'),
                    os.getenv('SCENARIO_TOPIC'),  # Add SCENARIO_TOPIC to listen for stop_processing messages
                    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    group_id="inference-group",
                    auto_offset_reset='latest',
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
            msg_type = message.get("type")
            scenario_id = message.get("scenario_id")

            if not scenario_id:
                logger.error("[Inference] Received message without scenario_id")
                return

            if msg_type == "stop_processing":
                logger.info(f"[Inference] Received stop processing command for scenario: {scenario_id}")
                # Stop heartbeat sender
                
                if scenario_id in self.heartbeat_senders:
                    self.heartbeat_senders[scenario_id].stop()
                    del self.heartbeat_senders[scenario_id]

                # Stop processing this scenario
                if scenario_id in self.active_scenarios:
                    self.active_scenarios.remove(scenario_id)
                    logger.info(f"[Inference] Stopped processing scenario: {scenario_id}")
                return
        except Exception as e:
            logger.error(f"[Inference] Something gone wrong while trying to stop processing: {str(e)}")
            
            
            if scenario_id not in self.active_scenarios:
                logger.warning(f"[Inference] Received frame for scenario {scenario_id} after it was stopped — skipping.")
                return
            
            frame = message.get("frame")
            frame_shape = message.get("frame_shape")
            frame_index = message.get("frame_index")

            if not all([frame, frame_shape, frame_index]):
                logger.error("[Inference] Received message without required frame data")
                return

            logger.info(f"[Inference] Starting processing for scenario: {scenario_id}")

            heartbeat_sender = HeartbeatSender(service_id="inference")
            heartbeat_sender.start_heartbeat(scenario_id)
            self.heartbeat_senders[scenario_id] = heartbeat_sender
            self.active_scenarios.add(scenario_id)

            try:
                # Decompress and process frame
                decompressed_frame = self.decompress_frame(frame, frame_shape)
                
                # Process frame with YOLO model
                results = self.yolo.predict(decompressed_frame)
                
                # Send predictions
                send_prediction(scenario_id, {
                    "scenario_id": scenario_id,
                    "frame_index": frame_index,
                    "predictions": results
                })
                
            except Exception as e:
                logger.error(f"[Inference] Error processing frame: {str(e)}")
                # Stop heartbeat sender
                if scenario_id in self.heartbeat_senders:
                    self.heartbeat_senders[scenario_id].stop()
                    del self.heartbeat_senders[scenario_id]

                # Make sure to remove scenario from active scenarios in case of error
                if scenario_id in self.active_scenarios:
                    self.active_scenarios.remove(scenario_id)
                raise

        except Exception as e:
            logger.error(f"[Inference] Error handling message: {str(e)}")
            # Stop heartbeat sender
            if scenario_id in self.heartbeat_senders:
                self.heartbeat_senders[scenario_id].stop()
                del self.heartbeat_senders[scenario_id]

            # Make sure to remove scenario from active scenarios in case of error
            if scenario_id in self.active_scenarios:
                self.active_scenarios.remove(scenario_id)
            raise

    def listen(self):
        logger.info("[KafkaConsumer] Starting to listen for Kafka messages...")
        while self.running:
            try:
                for msg in self.consumer:
                    if not self.running:
                        break
                    if msg.topic == os.getenv('INFERENCE_TOPIC'):
                        logger.info(f"[KafkaConsumer] Received message for frame {msg.value.get('frame_index')}")
                        self.handle_message(msg.value)
                    elif msg.topic == os.getenv('SCENARIO_TOPIC'):
                        logger.info(f"[KafkaConsumer] Received message from scenario topic: {msg.value}")
                        if msg.value.get('type') == 'stop_processing':
                            scenario_id = msg.value.get('scenario_id')
                            if scenario_id:
                                logger.info(f"[KafkaConsumer] Stopping processing for scenario {scenario_id}")
                                if scenario_id in self.heartbeat_senders:
                                    self.heartbeat_senders[scenario_id].stop()
                                    del self.heartbeat_senders[scenario_id]
                                if scenario_id in self.active_scenarios:
                                    self.active_scenarios.remove(scenario_id)
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

        for scenario_id, sender in self.heartbeat_senders.items():
            try:
                sender.stop()
            except Exception as e:
                logger.error(f"[KafkaConsumer] Error stopping heartbeat sender for scenario {scenario_id}: {str(e)}")
        self.heartbeat_senders.clear()

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

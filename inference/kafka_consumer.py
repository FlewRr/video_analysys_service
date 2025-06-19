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
from kafka_producer import send_prediction, notify_prediction_ready
from heartbeat import HeartbeatSender
import threading
from queue import Queue
from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import queue

logger = logging.getLogger(__name__)

# Database setup for predictions
DB_PATH = os.getenv('INFERENCE_DB_PATH', 'inference.db')
engine = create_engine(f'sqlite:///{DB_PATH}', echo=False)
Base = declarative_base()

class Prediction(Base):
    __tablename__ = 'predictions'
    id = Column(Integer, primary_key=True)
    scenario_id = Column(String, nullable=False)
    frame_index = Column(Integer, nullable=False)
    predictions = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)

def add_prediction(scenario_id: str, frame_index: int, predictions: list):
    db = SessionLocal()
    try:
        pred = Prediction(scenario_id=scenario_id, frame_index=frame_index, predictions=predictions)
        db.add(pred)
        db.commit()
        return pred
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()

class InferenceKafkaConsumer:
    _instance = None
    _yolo_initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(InferenceKafkaConsumer, cls).__new__(cls)
            cls._instance.consumer = None
            cls._instance.yolo = None
            cls._instance.heartbeat_senders = {}
            cls._instance.processing_threads = {}
            cls._instance.processing_flags = {}
            cls._instance.frame_queues = {}  # Queue for each scenario
            cls._instance.active_scenarios = set()
            cls._instance.running = True
            cls._instance.initialized = False
        return cls._instance

    def __init__(self):
        if not self.initialized:
            self.initialized = True
            logger.info("[KafkaConsumer] Initializing inference consumer...")
            self._connect_consumer()
            self._initialize_yolo()

    def _initialize_yolo(self):
        if not InferenceKafkaConsumer._yolo_initialized:
            try:
                logger.info("[KafkaConsumer] Initializing YOLO model...")
                self.yolo = YoloNano(device='cpu')
                InferenceKafkaConsumer._yolo_initialized = True
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
                    os.getenv('SCENARIO_TOPIC'),
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
        try:
            frame_bytes = base64.b64decode(compressed_frame)
            frame = cv2.imdecode(np.frombuffer(frame_bytes, np.uint8), cv2.IMREAD_COLOR)
            if frame is None:
                raise ValueError("Failed to decode frame from JPEG bytes")
            frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            return frame
        except Exception as e:
            logger.error(f"[Inference] Error decompressing frame: {str(e)}")
            raise

    def _process_frame(self, scenario_id: str, frame: str, frame_shape: tuple, frame_index: int):
        try:
            stop_event = self.processing_flags.get(scenario_id)
            if stop_event is None or stop_event.is_set():
                logger.info(f"[Inference] Processing stopped or not active for scenario {scenario_id}, skipping frame {frame_index}")
                return

            # Check stop flag before decompression
            if stop_event.is_set():
                return

            decompressed_frame = self.decompress_frame(frame, frame_shape)
            logger.info(f"[Inference] Processing frame {frame_index} for scenario {scenario_id}")

            # Check stop flag before YOLO prediction
            if stop_event.is_set():
                logger.info(f"[Inference] Processing interrupted before prediction for scenario {scenario_id}, frame {frame_index}")
                return

            results = self.yolo.predict(decompressed_frame)
            logger.info(f"[Inference] Found {len(results)} objects in frame {frame_index}")

            # Check stop flag before sending prediction
            if stop_event.is_set():
                logger.info(f"[Inference] Processing interrupted before sending prediction for scenario {scenario_id}, frame {frame_index}")
                return

            # Store prediction in database
            add_prediction(scenario_id, frame_index, results)
            # Instead of sending to runner, notify orchestrator via outbox
            notify_prediction_ready(scenario_id)
            logger.info(f"[Inference] Notified orchestrator that predictions are ready for scenario {scenario_id}")

        except Exception as e:
            logger.error(f"[Inference] Error processing frame: {str(e)}")

    def _process_scenario_frames(self, scenario_id: str):
        """Process frames for a specific scenario in a single thread"""
        logger.info(f"[Inference] Starting frame processing thread for scenario {scenario_id}")
        queue = self.frame_queues[scenario_id]
        stop_event = self.processing_flags[scenario_id]

        while not stop_event.is_set():
            try:
                # Get next frame from queue with shorter timeout
                frame_data = queue.get(timeout=0.1)  # Reduced timeout to check stop flag more frequently
                if frame_data is None:  # Poison pill
                    break

                # Check stop flag before processing frame
                if stop_event.is_set():
                    break

                frame, frame_shape, frame_index = frame_data
                self._process_frame(scenario_id, frame, frame_shape, frame_index)
                queue.task_done()

            except queue.Empty:
                # No frames in queue, check if we should continue
                if stop_event.is_set():
                    break
                continue
            except Exception as e:
                logger.error(f"[Inference] Error in scenario processing thread: {str(e)}")
                if stop_event.is_set():
                    break

        logger.info(f"[Inference] Stopping frame processing thread for scenario {scenario_id}")
        # Clean up
        self.frame_queues.pop(scenario_id, None)
        self.processing_threads.pop(scenario_id, None)

    def handle_message(self, message: dict):
        try:
            msg_type = message.get("type")
            scenario_id = message.get("scenario_id")

            if not scenario_id:
                logger.error("[Inference] Received message without scenario_id")
                return

            # Handle stop commands immediately with highest priority
            if msg_type == "stop_processing":
                logger.info(f"[Inference] Received stop processing command for scenario: {scenario_id}")
                # Stop heartbeat sender first
                if scenario_id in self.heartbeat_senders:
                    self.heartbeat_senders[scenario_id].stop()
                    del self.heartbeat_senders[scenario_id]
                
                # Clear frame queue and stop processing
                if scenario_id in self.frame_queues:
                    # Replace the queue with a new empty one
                    self.frame_queues[scenario_id] = Queue()
                    logger.info(f"[Inference] Cleared frame queue for scenario {scenario_id}")
                
                # Set stop flag
                if scenario_id in self.processing_flags:
                    self.processing_flags[scenario_id].set()
                
                self.active_scenarios.discard(scenario_id)
                logger.info(f"[Inference] Stopped processing for scenario {scenario_id}")
                return

            # Skip frame processing if scenario is not active
            if scenario_id not in self.active_scenarios:
                return

            if msg_type == "frame":
                frame = message.get("frame")
                frame_shape = message.get("frame_shape")
                frame_index = message.get("frame_index")

                if not all([frame, frame_shape, frame_index is not None]):
                    logger.error("[Inference] Missing frame data")
                    return

                # Initialize queue and processing thread if not already done
                if scenario_id not in self.frame_queues:
                    self.frame_queues[scenario_id] = Queue()
                    self.processing_flags[scenario_id] = threading.Event()
                    # Start processing thread immediately
                    thread = threading.Thread(
                        target=self._process_scenario_frames,
                        args=(scenario_id,),
                        daemon=True
                    )
                    self.processing_threads[scenario_id] = thread
                    thread.start()
                    logger.info(f"[Inference] Started frame processing thread for scenario {scenario_id}")

                # Add frame to queue
                self.frame_queues[scenario_id].put((frame, frame_shape, frame_index))
                logger.info(f"[Inference] Added frame {frame_index} to queue for scenario {scenario_id}")

        except Exception as e:
            logger.error(f"[Inference] Exception in handle_message: {str(e)}")

    def listen(self):
        logger.info("[KafkaConsumer] Starting to listen for Kafka messages...")
        while self.running:
            try:
                for msg in self.consumer:
                    if not self.running:
                        break

                    if msg.topic == os.getenv('INFERENCE_TOPIC'):
                        self.handle_message(msg.value)
                    elif msg.topic == os.getenv('SCENARIO_TOPIC'):
                        if msg.value.get('type') == 'start':
                            scenario_id = msg.value.get('scenario_id')
                            if scenario_id:
                                logger.info(f"[KafkaConsumer] Starting processing for scenario {scenario_id}")
                                self.active_scenarios.add(scenario_id)
                                self.processing_flags[scenario_id] = threading.Event()
                                if scenario_id not in self.heartbeat_senders:
                                    heartbeat_sender = HeartbeatSender(service_id="inference")
                                    heartbeat_sender.start_heartbeat(scenario_id)
                                    self.heartbeat_senders[scenario_id] = heartbeat_sender
            except Exception as e:
                logger.error(f"[KafkaConsumer] Error in message processing loop: {str(e)}")
                if self.running:
                    logger.info("[KafkaConsumer] Attempting to reconnect...")
                    time.sleep(5)
                    self._connect_consumer()

    def stop(self):
        self.running = False
        # Stop all scenario processing
        for scenario_id in list(self.processing_flags.keys()):
            self.processing_flags[scenario_id].set()
            if scenario_id in self.frame_queues:
                self.frame_queues[scenario_id].put(None)  # Poison pill

        # Stop all heartbeat senders
        for scenario_id, sender in self.heartbeat_senders.items():
            try:
                sender.stop()
            except Exception as e:
                logger.error(f"[KafkaConsumer] Error stopping heartbeat sender for scenario {scenario_id}: {str(e)}")

        # Clean up
        self.heartbeat_senders.clear()
        self.processing_threads.clear()
        self.frame_queues.clear()
        self.active_scenarios.clear()
        self.processing_flags.clear()

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

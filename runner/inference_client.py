import json
import logging
import cv2
import numpy as np
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
import time
import base64
import os
import threading

logger = logging.getLogger(__name__)

class InferenceClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(InferenceClient, cls).__new__(cls)
            cls._instance.producer = None
            cls._instance.total_frames = {}
            cls._instance.processing_flags = {}  # scenario_id: threading.Event()
            cls._instance.processing_threads = {}
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
                logger.info(f"[InferenceClient] Connecting to Kafka at {os.getenv('KAFKA_BOOTSTRAP_SERVERS')} (attempt {attempt + 1}/{max_retries})")
                self.producer = KafkaProducer(
                    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3
                )
                logger.info("[InferenceClient] Successfully connected to Kafka")
                return
            except (NoBrokersAvailable, KafkaError) as e:
                logger.warning(f"[InferenceClient] Kafka not available yet: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Could not connect to Kafka after {max_retries} attempts")

    def preprocess_frame(self, frame):
        resized = cv2.resize(frame, (640, 640))
        rgb = cv2.cvtColor(resized, cv2.COLOR_BGR2RGB)
        normalized = rgb.astype(np.float32) / 255.0
        return normalized

    def compress_frame(self, frame):
        try:
            frame_uint8 = (frame * 255).astype(np.uint8)
            success, buffer = cv2.imencode('.jpg', frame_uint8, [cv2.IMWRITE_JPEG_QUALITY, 95])
            if not success:
                raise ValueError("Failed to encode frame as JPEG")
            return base64.b64encode(buffer).decode('utf-8')
        except Exception as e:
            logger.error(f"[InferenceClient] Error compressing frame: {str(e)}")
            raise

    def get_total_frames(self, scenario_id: str) -> int:
        return self.total_frames.get(scenario_id)

    def _process_video_frames(self, scenario_id: str, video_path: str):
        try:
            cap = cv2.VideoCapture(video_path)
            if not cap.isOpened():
                raise ValueError(f"Could not open video file: {video_path}")

            fps = cap.get(cv2.CAP_PROP_FPS)
            frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            logger.info(f"[InferenceClient] Video FPS: {fps}, Total frames: {frame_count}")

            self.total_frames[scenario_id] = frame_count
            stop_event = threading.Event()
            self.processing_flags[scenario_id] = stop_event

            frame_index = 0
            processed_count = 0

            while not stop_event.is_set():
                ret, frame = cap.read()
                if not ret:
                    break

                # Process frame immediately
                processed_frame = self.preprocess_frame(frame)
                compressed_frame = self.compress_frame(processed_frame)

                message = {
                    "type": "frame",  # Add message type
                    "scenario_id": scenario_id,
                    "frame_index": frame_index,
                    "frame": compressed_frame,
                    "frame_shape": processed_frame.shape
                }

                future = self.producer.send(os.getenv('INFERENCE_TOPIC'), message)
                future.get(timeout=10)

                processed_count += 1
                if processed_count % 10 == 0:
                    logger.info(f"[InferenceClient] Processed {processed_count} frames (original frame {frame_index})")

                frame_index += 1

            cap.release()
            logger.info(f"[InferenceClient] Successfully processed {processed_count} frames out of {frame_count} for scenario {scenario_id}")

        except Exception as e:
            logger.error(f"[InferenceClient] Error processing video frames: {str(e)}")
            raise
        finally:
            self.processing_flags.pop(scenario_id, None)
            self.processing_threads.pop(scenario_id, None)
            self.total_frames.pop(scenario_id, None)

    def send_to_inference(self, scenario_id: str, video_path: str):
        logger.info(f"[InferenceClient] Starting frame processing thread for scenario {scenario_id}")
        processing_thread = threading.Thread(
            target=self._process_video_frames,
            args=(scenario_id, video_path),
            daemon=True
        )
        self.processing_threads[scenario_id] = processing_thread
        processing_thread.start()

    def stop_processing(self, scenario_id: str):
        try:
            logger.info(f"[InferenceClient] Stopping processing for scenario {scenario_id}")
            if scenario_id in self.processing_flags:
                self.processing_flags[scenario_id].set()

            if not self.producer:
                self._connect_producer()

            message = {
                "type": "stop_processing",
                "scenario_id": scenario_id,
                "timestamp": time.time()  # Add timestamp for ordering
            }

            # Send with highest priority and wait for confirmation
            future = self.producer.send(
                os.getenv('INFERENCE_TOPIC'),
                message,
                partition=0  # Use partition 0 for highest priority
            )
            future.get(timeout=5)  # Wait for confirmation
            self.producer.flush()  # Ensure message is sent
            logger.info(f"[InferenceClient] Sent stop processing command for scenario: {scenario_id}")

        except Exception as e:
            logger.error(f"[InferenceClient] Error sending stop processing command: {str(e)}")
            raise

    def close(self):
        if self.producer:
            try:
                self.producer.flush()
                self.producer.close()
                self.producer = None
                logger.info("[InferenceClient] Successfully closed producer")
            except Exception as e:
                logger.error(f"[InferenceClient] Error closing producer: {str(e)}")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

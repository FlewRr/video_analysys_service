import json
import logging
import cv2
import numpy as np
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
import time
import base64
import os

logger = logging.getLogger(__name__)

class InferenceClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(InferenceClient, cls).__new__(cls)
            cls._instance.producer = None
            cls._instance.total_frames = {}  # Track total frames per scenario
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
        """Preprocess frame for inference"""
        # Resize frame to match model input size
        resized = cv2.resize(frame, (640, 640))
        # Convert BGR to RGB
        rgb = cv2.cvtColor(resized, cv2.COLOR_BGR2RGB)
        # Normalize pixel values
        normalized = rgb.astype(np.float32) / 255.0
        return normalized

    def compress_frame(self, frame):
        """Compress frame data for efficient transmission"""
        try:
            # Convert float32 to uint8 for compression
            frame_uint8 = (frame * 255).astype(np.uint8)
            # Encode as JPEG with high quality
            success, buffer = cv2.imencode('.jpg', frame_uint8, [cv2.IMWRITE_JPEG_QUALITY, 95])
            if not success:
                raise ValueError("Failed to encode frame as JPEG")
            # Convert to base64 string
            compressed = base64.b64encode(buffer).decode('utf-8')
            return compressed
        except Exception as e:
            logger.error(f"[InferenceClient] Error compressing frame: {str(e)}")
            raise

    def get_total_frames(self, scenario_id: str) -> int:
        """Get total number of frames for a scenario"""
        return self.total_frames.get(scenario_id)

    def send_to_inference(self, scenario_id: str, video_path: str):
        """Send video frames to inference service"""
        logger.info(f"[InferenceClient] Preparing to process video {video_path} for scenario {scenario_id}")
        try:
            cap = cv2.VideoCapture(video_path)
            if not cap.isOpened():
                raise ValueError(f"Could not open video file: {video_path}")

            # Get video properties
            fps = cap.get(cv2.CAP_PROP_FPS)
            frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            logger.info(f"[InferenceClient] Video FPS: {fps}, Total frames: {frame_count}")

            # Store total frames for this scenario
            self.total_frames[scenario_id] = frame_count // 24  # Since we process every 24th frame

            # Sample every 24th frame (roughly 1 frame per second for 24fps video)
            frame_interval = 24
            frame_index = 0
            processed_count = 0

            while True:
                ret, frame = cap.read()
                if not ret:
                    break

                # Only process every 24th frame
                if frame_index % frame_interval == 0:
                    # Preprocess frame
                    processed_frame = self.preprocess_frame(frame)
                    
                    # Compress frame data
                    compressed_frame = self.compress_frame(processed_frame)

                    # Send frame to inference service
                    message = {
                        "scenario_id": scenario_id,
                        "frame_index": frame_index,
                        "frame": compressed_frame,
                        "frame_shape": processed_frame.shape
                    }
                    
                    future = self.producer.send(os.getenv('INFERENCE_TOPIC'), message)
                    future.get(timeout=10)  # Wait for the message to be delivered
                    
                    processed_count += 1
                    if processed_count % 10 == 0:  # Log progress every 10 processed frames
                        logger.info(f"[InferenceClient] Processed {processed_count} frames (original frame {frame_index})")

                frame_index += 1

            cap.release()
            logger.info(f"[InferenceClient] Successfully processed {processed_count} frames out of {frame_count} total frames for scenario {scenario_id}")

        except Exception as e:
            logger.error(f"[InferenceClient] Error sending to inference: {str(e)}")
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

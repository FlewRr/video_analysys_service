import json
import time
import logging
import os
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
from inference_client import InferenceClient
from kafka_producer import send_scenario_message
from heartbeat import HeartbeatSender

logger = logging.getLogger(__name__)

class KafkaListener:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(KafkaListener, cls).__new__(cls)
            cls._instance.consumer = None
            cls._instance.inference_client = None
            cls._instance.processed_frames_count = {}  # Track number of processed frames per scenario
            cls._instance.heartbeat_senders = {}  # Track heartbeat senders per scenario
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.running = True
            self._connect_consumer()
            self._connect_producer()
            self.inference_client = InferenceClient.get_instance()

    def _connect_producer(self):
        """Connect to Kafka producer"""
        max_retries = 30
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                logger.info("[KafkaListener] Successfully connected to Kafka producer")
                return
            except (NoBrokersAvailable, KafkaError) as e:
                logger.warning(f"[KafkaListener] Attempt {attempt + 1}/{max_retries}: Kafka producer not available yet: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Could not connect to Kafka producer after {max_retries} attempts")

    def _connect_consumer(self):
        max_retries = 30  # Increased retries for Docker environment
        retry_delay = 5   # Increased delay between retries
        
        for attempt in range(max_retries):
            try:
                self.consumer = KafkaConsumer(
                    os.getenv('RUNNER_TOPIC'),
                    os.getenv('SCENARIO_TOPIC'),  # Add SCENARIO_TOPIC to listen for shutdown messages
                    os.getenv('PREDICTION_TOPIC'),
                    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    group_id="runner-group",
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000,
                    max_poll_interval_ms=300000,  # 5 minutes
                    max_poll_records=1  # Process one record at a time
                )
                logger.info("[KafkaListener] Successfully connected to Kafka")
                return
            except (NoBrokersAvailable, KafkaError) as e:
                logger.warning(f"[KafkaListener] Attempt {attempt + 1}/{max_retries}: Kafka not available yet: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Could not connect to Kafka after {max_retries} attempts")

    def handle_message(self, message):
        """Handle incoming Kafka messages"""
        try:
            msg_type = message.get('type')
            scenario_id = message.get('scenario_id')

            if not scenario_id:
                logger.error("[Runner] Received message without scenario_id")
                return

            if msg_type == 'start':
                video_path = message.get('video_path')
                if not video_path:
                    logger.error("[Runner] Received start command without video_path")
                    return

                logger.info(f"[Runner] Starting processing for video: {video_path}")
                try:
                    # Start heartbeat sender for this scenario
                    heartbeat_sender = HeartbeatSender(service_id="runner")
                    heartbeat_sender.start_heartbeat(scenario_id)
                    self.heartbeat_senders[scenario_id] = heartbeat_sender

                    # Try to process the video
                    self.inference_client.send_to_inference(scenario_id, video_path)
                except FileNotFoundError as e:
                    # Stop heartbeat sender
                    if scenario_id in self.heartbeat_senders:
                        self.heartbeat_senders[scenario_id].stop()
                        del self.heartbeat_senders[scenario_id]

                    # Send error message to orchestrator
                    error_msg = {
                        "type": "error",
                        "scenario_id": scenario_id,
                        "error": str(e)
                    }
                    self.producer.send(os.getenv('ORCHESTRATOR_TOPIC'), error_msg)
                    logger.error(f"[Runner] Error processing video: {str(e)}")
                    # Clean up tracking
                    if scenario_id in self.processed_frames_count:
                        del self.processed_frames_count[scenario_id]
                    return
                except Exception as e:
                    # Stop heartbeat sender
                    if scenario_id in self.heartbeat_senders:
                        self.heartbeat_senders[scenario_id].stop()
                        del self.heartbeat_senders[scenario_id]

                    logger.error(f"[Runner] Error processing video: {str(e)}")
                    # Clean up tracking
                    if scenario_id in self.processed_frames_count:
                        del self.processed_frames_count[scenario_id]
                    return

            elif msg_type == 'shutdown':
                logger.info(f"[Runner] Received shutdown command for scenario: {scenario_id}")
                # Stop heartbeat sender
                if scenario_id in self.heartbeat_senders:
                    self.heartbeat_senders[scenario_id].stop()
                    del self.heartbeat_senders[scenario_id]

                # Notify inference service to stop processing
                self.inference_client.stop_processing(scenario_id)
                logger.info(f"[Runner] Notified inference service to stop processing scenario: {scenario_id}")
                
                # Clean up tracking
                if scenario_id in self.processed_frames_count:
                    del self.processed_frames_count[scenario_id]

            elif "predictions" in message:  # Handle prediction message
                predictions = message.get("predictions", {})
                frame_index = predictions.get("frame_index")
                
                if frame_index is not None:
                    # Increment processed frames counter
                    if scenario_id not in self.processed_frames_count:
                        self.processed_frames_count[scenario_id] = 0
                    self.processed_frames_count[scenario_id] += 1
                    
                    # Check if all frames are processed
                    total_frames = self.inference_client.get_total_frames(scenario_id)
                    if total_frames and self.processed_frames_count[scenario_id] >= total_frames:
                        logger.info(f"[Runner] All frames processed for scenario {scenario_id}, sending shutdown message")
                        send_scenario_message({
                            "type": "shutdown",
                            "scenario_id": scenario_id
                        })
                        # Clean up tracking
                        del self.processed_frames_count[scenario_id]

        except Exception as e:
            logger.error(f"[Runner] Error handling message: {str(e)}")
            # Clean up tracking
            if scenario_id in self.processed_frames_count:
                del self.processed_frames_count[scenario_id]

    def listen(self):
        logger.info("[KafkaListener] Starting to listen for Kafka messages...")
        while self.running:
            try:
                for msg in self.consumer:
                    if not self.running:
                        break
                    try:
                        logger.info(f"[RUNNER] GOT MESSAGE: {msg.value}")
                        message = msg.value
                        self.handle_message(message)
                        # Commit offset after successful processing
                        self.consumer.commit()
                    except Exception as e:
                        logger.error(f"[KafkaListener] Error processing message: {str(e)}")
                        # Don't re-raise to continue processing other messages
                        continue
            except Exception as e:
                logger.error(f"[KafkaListener] Error in message processing loop: {str(e)}")
                if self.running:
                    logger.info("[KafkaListener] Attempting to reconnect...")
                    time.sleep(5)
                    self._connect_consumer()

    def stop(self):
        self.running = False
        # Stop all heartbeat senders
        for scenario_id, sender in self.heartbeat_senders.items():
            try:
                sender.stop()
            except Exception as e:
                logger.error(f"[KafkaListener] Error stopping heartbeat sender for scenario {scenario_id}: {str(e)}")
        self.heartbeat_senders.clear()

        if self.consumer:
            try:
                self.consumer.close()
                logger.info("[KafkaListener] Successfully closed consumer")
            except Exception as e:
                logger.error(f"[KafkaListener] Error closing consumer: {str(e)}")
        if self.producer:
            try:
                self.producer.close()
                logger.info("[KafkaListener] Successfully closed producer")
            except Exception as e:
                logger.error(f"[KafkaListener] Error closing producer: {str(e)}")
        if self.inference_client:
            self.inference_client.close()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

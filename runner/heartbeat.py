import os
import time
import logging
import threading
import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

logger = logging.getLogger(__name__)

class HeartbeatSender:
    def __init__(self, service_id: str):
        self.service_id = service_id
        self.producer = None
        self.heartbeat_thread = None
        self.running = True
        self._connect_producer()

    def _connect_producer(self):
        max_retries = 30
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1
                )
                logger.info(f"[HeartbeatSender] Successfully connected to Kafka for service {self.service_id}")
                return
            except (NoBrokersAvailable, KafkaError) as e:
                logger.warning(f"[HeartbeatSender] Attempt {attempt + 1}/{max_retries}: Kafka not available yet: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Could not connect to Kafka after {max_retries} attempts")

    def send_heartbeat(self, scenario_id: str):
        """Send a heartbeat message for a specific scenario"""
        if not self.running:
            logger.debug(f"[HeartbeatSender] Not sending heartbeat as sender is stopping")
            return

        try:
            if not self.producer:
                logger.error("[HeartbeatSender] Producer not initialized")
                self._connect_producer()

            message = {
                "scenario_id": scenario_id,
                "service_id": self.service_id,
                "timestamp": time.time()
            }

            logger.info(f"[HeartbeatSender] Sending heartbeat message: {message}")
            self.producer.send(os.getenv('HEARTBEAT_TOPIC'), message)
            self.producer.flush()
            logger.info(f"[HeartbeatSender] Successfully sent heartbeat for scenario {scenario_id} from service {self.service_id}")
        except Exception as e:
            logger.error(f"[HeartbeatSender] Error sending heartbeat: {str(e)}")

    def start_heartbeat(self, scenario_id: str):
        """Start sending heartbeats for a scenario"""
        def heartbeat_loop():
            logger.info(f"[HeartbeatSender] Starting heartbeat loop for scenario {scenario_id} from service {self.service_id}")
            while self.running:
                try:
                    if not self.running:  # Double check before sending
                        break
                    self.send_heartbeat(scenario_id)
                except Exception as e:
                    logger.error(f"[HeartbeatSender] Error in heartbeat loop: {str(e)}")
                if self.running:  # Only sleep if still running
                    logger.info(f"[HeartbeatSender] Waiting 10 seconds before next heartbeat for scenario {scenario_id}")
                    time.sleep(10)  # Send heartbeat every 10 seconds

        self.heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
        logger.info(f"[HeartbeatSender] Started heartbeat thread for scenario {scenario_id} from service {self.service_id}")

    def stop(self):
        """Stop sending heartbeats"""
        logger.info(f"[HeartbeatSender] Stopping heartbeat sender for service {self.service_id}")
        self.running = False
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=1)  # Reduced timeout to stop faster
        if self.producer:
            try:
                self.producer.flush()  # Ensure any pending messages are sent
                self.producer.close()
            except Exception as e:
                logger.error(f"[HeartbeatSender] Error closing producer: {str(e)}")
        logger.info(f"[HeartbeatSender] Stopped heartbeat sender for service {self.service_id}") 
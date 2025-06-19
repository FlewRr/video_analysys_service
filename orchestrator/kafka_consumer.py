import json
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
import time
import logging
import os
import sqlite3
from storage import SessionLocal, get_scenario, update_scenario_state, update_scenario_predictions
from state_machine import can_transition
from kafka_producer import send_runner_command
from datetime import datetime
from storage import Scenario
import requests

logger = logging.getLogger(__name__)


class KafkaListener:
    def __init__(self):
        self.consumer = None
        self._connect_consumer()
        self.running = True

    def _connect_consumer(self):
        max_retries = 30  # Increased retries for Docker environment
        retry_delay = 5   # Increased delay between retries
        
        for attempt in range(max_retries):
            try:
                logger.info(f"[KafkaListener] Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})")
                topics = [
                    os.getenv('SCENARIO_TOPIC'),
                    os.getenv('PREDICTION_TOPIC'),
                    os.getenv('HEARTBEAT_TOPIC')
                ]
                logger.info(f"[KafkaListener] Subscribing to topics: {topics}")
                
                self.consumer = KafkaConsumer(
                    *topics,
                    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    group_id="orchestrator-group",
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000
                )
                logger.info("[KafkaListener] Successfully connected to Kafka")
                return
            except (NoBrokersAvailable, KafkaError) as e:
                logger.warning(f"[KafkaListener] Attempt {attempt + 1}/{max_retries}: Kafka not available yet: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Could not connect to Kafka after {max_retries} attempts")

    def handle_scenario_message(self, message):
        session = SessionLocal()
        try:
            message_type = message.get("type")
            scenario_id = message.get("scenario_id")

            if not scenario_id:
                logger.error("[KafkaListener] Received message without scenario_id")
                return

            scenario = get_scenario(session, scenario_id)
            if not scenario:
                logger.error(f"[KafkaListener] Scenario {scenario_id} not found")
                return

            if message_type == "error":
                logger.error(f"[KafkaListener] Received error for scenario {scenario_id}: {message.get('error')}")
                # Transition directly to in_shutdown_processing state
                if scenario.state in ["active", "in_startup_processing"]:
                    update_scenario_state(session, scenario_id, "in_shutdown_processing")
                    session.commit()
                    logger.info(f"[KafkaListener] Updated scenario {scenario_id} state to in_shutdown_processing due to error")

                    # Send shutdown command to stop all services
                    send_runner_command({
                        "type": "shutdown",
                        "scenario_id": scenario_id
                    })

                    # Mark as inactive after shutdown
                    update_scenario_state(session, scenario_id, "inactive")
                    session.commit()
                    logger.info(f"[KafkaListener] Updated scenario {scenario_id} state to inactive after error")

            elif message_type == "start":
                logger.info(f"[KafkaListener] Received start message for scenario {scenario_id} in state {scenario.state}")
                if scenario.state in ["active", "inactive", "init_startup"]:
                    logger.info(f"[KafkaListener] Transitioning scenario {scenario_id} from {scenario.state} to in_startup_processing")
                    update_scenario_state(session, scenario_id, "in_startup_processing")
                    session.commit()

                    scenario = get_scenario(session, scenario_id)
                    if not scenario or not scenario.video_path:
                        logger.error(f"[KafkaListener] No video_path found for scenario {scenario_id}")
                        update_scenario_state(session, scenario_id, "inactive")
                        session.commit()
                        return

                    logger.info(f"[KafkaListener] Sending start command to runner for scenario {scenario_id}")
                    send_runner_command({
                        "type": "start",
                        "scenario_id": scenario_id,
                        "video_path": scenario.video_path
                    })

                    logger.info(f"[KafkaListener] Transitioning scenario {scenario_id} from in_startup_processing to active")
                    update_scenario_state(session, scenario_id, "active")
                    session.commit()

                    logger.info(f"[KafkaListener] Successfully updated scenario {scenario_id} state to active")
                else:
                    logger.warning(f"[KafkaListener] Received start message for scenario {scenario_id} in unexpected state {scenario.state}")

            elif message_type == "shutdown":
                # Handle scenario shutdown
                if scenario.state == "active":
                    update_scenario_state(session, scenario_id, "init_shutdown")
                    session.commit()

                    update_scenario_state(session, scenario_id, "in_shutdown_processing")
                    session.commit()

                    logger.info("[Orchestrator] About to send runner a command!!!!!!")
                    send_runner_command({
                        "type": "shutdown",
                        "scenario_id": scenario_id
                    })

                    update_scenario_state(session, scenario_id, "inactive")
                    session.commit()

                    logger.info(f"[KafkaListener] Updated scenario {scenario_id} state to inactive")

            elif message_type == "state_change_request":
                new_state = message.get("new_state")
                if not new_state:
                    logger.error("[KafkaListener] State change request without new_state")
                    return
                
                # Check if state transition is allowed
                if can_transition(scenario.state, new_state):
                    # Update state in database
                    update_scenario_state(session, scenario_id, new_state)
                    session.commit()
                    logger.info(f"[KafkaListener] Updated scenario {scenario_id} state to {new_state}")
                else:
                    logger.warning(f"[KafkaListener] Invalid state transition from {scenario.state} to {new_state}")

        except Exception as e:
            logger.error(f"[KafkaListener] Error handling scenario message: {str(e)}")
            session.rollback()
        finally:
            session.close()

    def handle_prediction_message(self, message):
        payload = message.get('payload', {})
        scenario_id = payload.get('scenario_id')
        if scenario_id:
            # Fetch predictions from inference API
            try:
                INFERENCE_API = os.getenv('INFERENCE_API_URL', 'http://inference:8003')
                resp = requests.get(f"{INFERENCE_API}/predictions/{scenario_id}")
                if resp.status_code == 200:
                    predictions = resp.json().get('predictions', [])
                    session = SessionLocal()
                    update_scenario_predictions(session, scenario_id, predictions)
                    session.close()
                    logger.info(f"[KafkaListener] Updated scenario {scenario_id} predictions from inference DB")
                else:
                    logger.error(f"[KafkaListeners] Failed to fetch predictions from inference for scenario {scenario_id}: {resp.text}")
            except Exception as e:
                logger.error(f"[KafkaListener] Exception fetching predictions from inference: {str(e)}")

    def handle_heartbeat_message(self, message):
        """Handle heartbeat messages from services"""
        try:
            scenario_id = message.get("scenario_id")
            service_id = message.get("service_id")
            
            if not scenario_id or not service_id:
                logger.error("[KafkaListener] Received heartbeat message without scenario_id or service_id")
                return
            
            logger.info(f"[KafkaListener] Received heartbeat message: {message}")
            from heartbeat import heartbeat_monitor
            heartbeat_monitor.update_heartbeat(scenario_id, service_id)
            
        except Exception as e:
            logger.error(f"[KafkaListener] Error handling heartbeat message: {str(e)}")

    def listen(self):
        logger.info("[KafkaListener] Starting to listen for messages")
        while self.running:
            try:
                for msg in self.consumer:
                    if not self.running:
                        break

                    topic = msg.topic
                    message = msg.value

                    logger.info(f"[KafkaListener] Received message on topic {topic}: {message}")

                    # Handle prediction_ready event from inference
                    if topic == os.getenv('PREDICTION_TOPIC'):
                        self.handle_prediction_message(message)
                    elif topic == os.getenv('SCENARIO_TOPIC'):
                        self.handle_scenario_message(message)
                    elif topic == os.getenv('HEARTBEAT_TOPIC'):
                        self.handle_heartbeat_message(message)
                    else:
                        logger.warning(f"[KafkaListener] Received message from unknown topic: {topic}")

            except Exception as e:
                logger.error(f"[KafkaListener] Error in message loop: {str(e)}")
                time.sleep(5)  # Wait before retrying

    def stop(self):
        self.running = False
        if self.consumer:
            self.consumer.close()

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
from outbox import create_outbox_event

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
                self.consumer = KafkaConsumer(
                    os.getenv('SCENARIO_TOPIC'),
                    os.getenv('PREDICTION_TOPIC'),
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

            if message_type == "start":
                if scenario.state == "init_startup":
                    update_scenario_state(session, scenario_id, "in_startup_processing")
                    session.commit()

                    create_outbox_event(
                        event_type='scenario_state_changed',
                        payload={
                            "scenario_id": scenario_id,
                            "new_state": "in_startup_processing",
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )

                    send_runner_command({
                        "type": "start",
                        "scenario_id": scenario_id,
                        "video_path": scenario.video_path
                    })

                    update_scenario_state(session, scenario_id, "active")
                    session.commit()

                    create_outbox_event(
                        event_type='scenario_state_changed',
                        payload={
                            "scenario_id": scenario_id,
                            "new_state": "active",
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )

                    logger.info(f"[KafkaListener] Updated scenario {scenario_id} state to in_startup_processing")

            elif message_type == "shutdown":
                # Handle scenario shutdown
                if scenario.state == "active":
                    update_scenario_state(session, scenario_id, "init_shutdown")
                    session.commit()
                    # Create outbox event for state change
                    create_outbox_event(
                        event_type='scenario_state_changed',
                        payload={
                            "scenario_id": scenario_id,
                            "new_state": "init_shutdown",
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )

                    update_scenario_state(session, scenario_id, "in_shutdown_processing")
                    session.commit()
                    # Create outbox event for state change
                    create_outbox_event(
                        event_type='scenario_state_changed',
                        payload={
                            "scenario_id": scenario_id,
                            "new_state": "in_shutdown_processing",
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )

                    send_runner_command({
                        "type": "shutdown",
                        "scenario_id": scenario_id
                    })

                    update_scenario_state(session, scenario_id, "inactive")
                    session.commit()
                    # Create outbox event for state change
                    create_outbox_event(
                        event_type='scenario_state_changed',
                        payload={
                            "scenario_id": scenario_id,
                            "new_state": "inactive",
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )

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
                    # Create outbox event for state change
                    create_outbox_event(
                        event_type='scenario_state_changed',
                        payload={
                            "scenario_id": scenario_id,
                            "new_state": new_state,
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )
                    logger.info(f"[KafkaListener] Updated scenario {scenario_id} state to {new_state}")
                else:
                    logger.warning(f"[KafkaListener] Invalid state transition from {scenario.state} to {new_state}")

        except Exception as e:
            logger.error(f"[KafkaListener] Error handling scenario message: {str(e)}")
            session.rollback()
        finally:
            session.close()

    def handle_prediction_message(self, message):
        session = SessionLocal()
        try:
            scenario_id = message.get("scenario_id")
            predictions = message.get("predictions", {})

            if not scenario_id:
                logger.error("[KafkaListener] Received prediction without scenario_id")
                return

            scenario = get_scenario(session, scenario_id)
            if not scenario:
                logger.error(f"[KafkaListener] Scenario {scenario_id} not found")
                return

            # Initialize predictions list if it doesn't exist
            if scenario.predictions is None:
                scenario.predictions = []
                logger.info(f"[KafkaListener] Initialized empty predictions list for {scenario_id}")
            
            # Add new prediction to the list
            scenario.predictions.append(predictions)
            scenario.updated_at = datetime.utcnow()
            
            # Explicitly update the predictions column
            session.query(Scenario).filter(Scenario.id == scenario_id).update({
                "predictions": scenario.predictions,
                "updated_at": scenario.updated_at
            })
            
            session.commit()
            logger.info(f"[KafkaListener] Successfully committed prediction update for {scenario_id}")
            
            # Verify the update
            session.refresh(scenario)
        except Exception as e:
            logger.error(f"[KafkaListener] Error updating predictions in database: {str(e)}")
            session.rollback()
            raise

        except Exception as e:
            logger.error(f"[KafkaListener] Error handling prediction message: {str(e)}")
            session.rollback()
        finally:
            session.close()

    def listen(self):
        logger.info("[KafkaListener] Starting to listen for messages")
        while self.running:
            try:
                for msg in self.consumer:
                    if not self.running:
                        break

                    topic = msg.topic
                    message = msg.value

                    if topic == os.getenv('SCENARIO_TOPIC'):
                        self.handle_scenario_message(message)
                    elif topic == os.getenv('PREDICTION_TOPIC'):
                        self.handle_prediction_message(message)

            except Exception as e:
                logger.error(f"[KafkaListener] Error in message loop: {str(e)}")
                time.sleep(5)  # Wait before retrying

    def stop(self):
        self.running = False
        if self.consumer:
            self.consumer.close()

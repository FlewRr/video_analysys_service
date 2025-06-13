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
from outbox import Outbox
from datetime import datetime
from storage import Scenario

logger = logging.getLogger(__name__)

def log_database_info():
    try:
        # Get database file info
        db_path = '/db'
        if os.path.exists(db_path):
            size = os.path.getsize(db_path)
            logger.info(f"[KafkaListener] Database file exists at {db_path}, size: {size} bytes")
            
            # Check if we can connect to the database
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Get list of tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            logger.info(f"[KafkaListener] Database tables: {[table[0] for table in tables]}")
            
            # Get count of scenarios
            cursor.execute("SELECT COUNT(*) FROM scenarios;")
            count = cursor.fetchone()[0]
            logger.info(f"[KafkaListener] Number of scenarios in database: {count}")
            
            # Get all scenario IDs
            cursor.execute("SELECT id FROM scenarios;")
            scenario_ids = cursor.fetchall()
            logger.info(f"[KafkaListener] Scenario IDs in database: {[id[0] for id in scenario_ids]}")
            
            conn.close()
        else:
            logger.error(f"[KafkaListener] Database file does not exist at {db_path}")
    except Exception as e:
        logger.error(f"[KafkaListener] Error checking database info: {str(e)}")

class KafkaListener:
    def __init__(self):
        self.consumer = None
        self._connect_consumer()
        self.running = True
        # Log database info on startup
        log_database_info()

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

            # Log database info before querying
            log_database_info()
            
            scenario = get_scenario(session, scenario_id)
            if not scenario:
                logger.error(f"[KafkaListener] Scenario {scenario_id} not found")
                return

            if message_type == "start":
                # Handle scenario start
                if scenario.state == "init_startup":
                    # Send command to runner
                    send_runner_command({
                        "type": "start",
                        "scenario_id": scenario_id,
                        "video_path": scenario.video_path
                    })
                    # Update state
                    update_scenario_state(session, scenario_id, "in_startup_processing")
                    logger.info(f"[KafkaListener] Updated scenario {scenario_id} state to in_startup_processing")

            elif message_type == "state_change_request":
                new_state = message.get("new_state")
                if not new_state:
                    logger.error("[KafkaListener] State change request without new_state")
                    return

                # Check if state transition is allowed
                if can_transition(scenario.state, new_state):
                    # Update state in database
                    update_scenario_state(session, scenario_id, new_state)
                    # Notify other services about state change
                    Outbox.get_instance().send_scenario_update(scenario_id, new_state)
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
            predictions = message.get("predictions")

            if not scenario_id or not predictions:
                logger.error("[KafkaListener] Invalid prediction message")
                return

            logger.info(f"[KafkaListener] Received prediction for {scenario_id}: {predictions}")
            
            scenario = get_scenario(session, scenario_id)
            if not scenario:
                logger.error(f"[KafkaListener] Scenario {scenario_id} not found")
                return

            logger.info(f"[KafkaListener] Before update - Current predictions for {scenario_id}: {scenario.predictions}")

            try:
                # Initialize predictions as a list if it doesn't exist
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
                logger.info(f"[KafkaListener] After update - Verified predictions for {scenario_id}: {scenario.predictions}")
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

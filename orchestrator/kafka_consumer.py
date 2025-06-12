import json
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
import time
import logging
from config import KAFKA_BOOTSTRAP_SERVERS, SCENARIO_TOPIC, PREDICTION_TOPIC
from scenarios import create_scenario, update_scenario_state, set_predictions, get_scenario
from state_machine import can_transition
from kafka_producer import send_runner_command

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
                    SCENARIO_TOPIC,
                    PREDICTION_TOPIC,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
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
        try:
            msg_type = message.get("type")
            scenario_id = message.get("scenario_id")

            if not scenario_id:
                logger.error("[Orchestrator] Received message without scenario_id")
                return

            if msg_type == "start":
                video_path = message.get("video_path")
                logger.info(f"[Orchestrator] Started processing for video: {video_path}")
                try:
                    create_scenario(scenario_id, video_path)

                    update_scenario_state({
                        "scenario_id": scenario_id,
                        "new_state": "init_startup"
                    })
                    logger.info(f"[Orchestrator] Changed state to init_startup")

                    update_scenario_state({
                        "scenario_id": scenario_id,
                        "new_state": "in_startup_processing"
                    })
                    logger.info(f"[Orchestrator] Changed state to in_startup_processing")

                    send_runner_command({
                        "type": "start",
                        "scenario_id": scenario_id,
                        "video_path": video_path
                    })
                    logger.info(f"[Orchestrator] Runner is told to start processing")

                except ValueError as e:
                    logger.error(f"[Orchestrator] Error processing start command: {str(e)}")

            elif msg_type == "state_change":
                new_state = message.get("new_state")
                scenario = get_scenario(scenario_id)
                if not scenario:
                    logger.error(f"[Orchestrator] Scenario {scenario_id} not found for state_change")
                    return
                current_state = scenario["state"]
                if can_transition(current_state, new_state):
                    update_scenario_state({
                        "scenario_id": scenario_id,
                        "new_state": new_state
                    })

                    if new_state == "init_shutdown":
                        send_runner_command({
                            "type": "shutdown",
                            "scenario_id": scenario_id
                        })
                else:
                    logger.warning(f"[Orchestrator] Invalid state transition from {current_state} to {new_state} for {scenario_id}")
        except Exception as e:
            logger.error(f"[Orchestrator] Error handling scenario message: {str(e)}")

    def handle_prediction_message(self, message):
        try:
            scenario_id = message.get("scenario_id")
            predictions = message.get("predictions")
            if scenario_id and predictions:
                set_predictions(scenario_id, predictions)
                scenario = get_scenario(scenario_id)
                if scenario and scenario["state"] == "in_startup_processing":
                    update_scenario_state({
                        "scenario_id": scenario_id,
                        "new_state": "active"
                    })
        except Exception as e:
            logger.error(f"[Orchestrator] Error handling prediction message: {str(e)}")

    def listen(self):
        logger.info("[KafkaListener] Starting to listen for Kafka messages...")
        while self.running:
            try:
                for msg in self.consumer:
                    if not self.running:
                        break
                    topic = msg.topic
                    message = msg.value

                    if topic == SCENARIO_TOPIC:
                        self.handle_scenario_message(message)
                    elif topic == PREDICTION_TOPIC:
                        self.handle_prediction_message(message)
            except Exception as e:
                logger.error(f"[KafkaListener] Error in message processing loop: {str(e)}")
                if self.running:
                    logger.info("[KafkaListener] Attempting to reconnect...")
                    time.sleep(5)
                    self._connect_consumer()

    def stop(self):
        self.running = False
        if self.consumer:
            self.consumer.close()

import json
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time
from config import KAFKA_BOOTSTRAP_SERVERS, SCENARIO_TOPIC, PREDICTION_TOPIC
from scenarios import create_scenario, update_scenario_state, set_predictions, get_scenario
from state_machine import can_transition
from kafka_producer import send_runner_command

class KafkaListener:
    def __init__(self):
        self.consumer = None
        self._connect_consumer()

    def _connect_consumer(self):
        for _ in range(10):
            try:
                self.consumer = KafkaConsumer(
                    SCENARIO_TOPIC,
                    PREDICTION_TOPIC,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    group_id="orchestrator-group"
                )
                print("[KafkaListener] Connected to Kafka")
                break
            except NoBrokersAvailable:
                print("[KafkaListener] Kafka not available yet, retrying...")
                time.sleep(3)
        else:
            raise Exception("Could not connect to Kafka after retrying")

    def handle_scenario_message(self, message):
        msg_type = message.get("type")
        scenario_id = message.get("scenario_id")

        if msg_type == "start":
            video_path = message.get("video_path")
            print(f"[Orchestrator] Started processing for video: {video_path}")
            try:
                create_scenario(scenario_id, video_path)

                update_scenario_state({
                    "scenario_id": scenario_id,
                    "new_state": "init_startup"
                })
                print(f"[Orchestrator] Changed state to init_startup")

                update_scenario_state({
                    "scenario_id": scenario_id,
                    "new_state": "in_startup_processing"
                })
                print(f"[Orchestrator] Changed state to in_startup_processing")

                send_runner_command({
                    "type": "start",
                    "scenario_id": scenario_id,
                    "video_path": video_path
                })
                print(f"[Orchestrator] Runner is told to start processing")

            except ValueError:
                print(f"[Orchestrator] Scenario {scenario_id} already exists. Ignoring start command.")

        elif msg_type == "state_change":
            new_state = message.get("new_state")
            scenario = get_scenario(scenario_id)
            if not scenario:
                print(f"[Orchestrator] Scenario {scenario_id} not found for state_change")
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
                print(f"[Orchestrator] Invalid state transition from {current_state} to {new_state} for {scenario_id}")

    def handle_prediction_message(self, message):
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

    def listen(self):
        print("[KafkaListener] Listening for Kafka messages...")
        for msg in self.consumer:
            topic = msg.topic
            message = msg.value

            if topic == SCENARIO_TOPIC:
                self.handle_scenario_message(message)
            elif topic == PREDICTION_TOPIC:
                self.handle_prediction_message(message)

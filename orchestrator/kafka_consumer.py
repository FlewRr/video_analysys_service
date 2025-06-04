import json
from kafka import KafkaConsumer
from orchestrator.config import KAFKA_BOOTSTRAP_SERVERS, SCENARIO_TOPIC, PREDICTION_TOPIC
from orchestrator.scenario_store import create_scenario, update_scenario_state, set_predictions, get_scenario
from orchestrator.state_machine import can_transition
from orchestrator.kafka_producer import send_runner_command

consumer = KafkaConsumer(
    SCENARIO_TOPIC,
    PREDICTION_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id="orchestrator-group"
)

def handle_scenario_message(message):
    msg_type = message.get("type")
    scenario_id = message.get("scenario_id")

    if msg_type == "start":
        video_path = message.get("video_path")
        try:
            create_scenario(scenario_id, video_path)
            update_scenario_state(scenario_id, "init_startup")
            # Move to in_startup_processing
            update_scenario_state(scenario_id, "in_startup_processing")
            # Tell runner to start processing video
            send_runner_command({
                "type": "start",
                "scenario_id": scenario_id,
                "video_path": video_path
            })
        except ValueError:
            # Scenario exists - ignore or log
            pass

    elif msg_type == "state_change":
        new_state = message.get("new_state")
        scenario = get_scenario(scenario_id)
        if not scenario:
            return
        current_state = scenario["state"]
        if can_transition(current_state, new_state):
            update_scenario_state(scenario_id, new_state)

            # If entering shutdown phases, notify runner accordingly
            if new_state == "init_shutdown":
                send_runner_command({
                    "type": "shutdown",
                    "scenario_id": scenario_id
                })
        else:
            print(f"[Orchestrator] Invalid state transition from {current_state} to {new_state} for {scenario_id}")

def handle_prediction_message(message):
    scenario_id = message.get("scenario_id")
    predictions = message.get("predictions")
    if scenario_id and predictions:
        set_predictions(scenario_id, predictions)
        # Optionally, update scenario state to active or whatever logic you want
        scenario = get_scenario(scenario_id)
        if scenario and scenario["state"] == "in_startup_processing":
            update_scenario_state(scenario_id, "active")

def listen():
    print("[Orchestrator] Listening for Kafka messages...")
    for msg in consumer:
        topic = msg.topic
        message = msg.value

        if topic == SCENARIO_TOPIC:
            handle_scenario_message(message)
        elif topic == PREDICTION_TOPIC:
            handle_prediction_message(message)

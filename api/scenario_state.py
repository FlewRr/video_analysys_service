# Simple in-memory scenario state store

import json
import logging
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
from config import KAFKA_BOOTSTRAP_SERVERS, SCENARIO_TOPIC, PREDICTION_TOPIC
import threading
import time

logger = logging.getLogger(__name__)

# In-memory state store
_SCENARIOS = {}

def _connect_consumer():
    max_retries = 30
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                SCENARIO_TOPIC,
                PREDICTION_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id="api-group",
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            logger.info("[API] Successfully connected to Kafka")
            return consumer
        except (NoBrokersAvailable, KafkaError) as e:
            logger.warning(f"[API] Attempt {attempt + 1}/{max_retries}: Kafka not available yet: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise Exception(f"Could not connect to Kafka after {max_retries} attempts")

def _listen_for_updates():
    consumer = _connect_consumer()
    while True:
        try:
            for msg in consumer:
                message = msg.value
                topic = msg.topic

                if topic == SCENARIO_TOPIC and message.get("type") == "state_change":
                    scenario_id = message.get("scenario_id")
                    new_state = message.get("new_state")
                    if scenario_id and new_state:
                        if scenario_id not in _SCENARIOS:
                            logger.warning(f"[API] Received state change for unknown scenario {scenario_id}")
                            continue
                        _SCENARIOS[scenario_id]["state"] = new_state
                        logger.info(f"[API] Updated scenario {scenario_id} state to {new_state}")
                
                elif topic == PREDICTION_TOPIC:
                    scenario_id = message.get("scenario_id")
                    prediction_data = message.get("predictions")
                    if scenario_id and prediction_data:
                        if scenario_id not in _SCENARIOS:
                            logger.warning(f"[API] Received predictions for unknown scenario {scenario_id}")
                            continue
                        
                        # Initialize predictions dictionary if it doesn't exist
                        if _SCENARIOS[scenario_id]["predictions"] is None:
                            _SCENARIOS[scenario_id]["predictions"] = {}
                        
                        # Add or update prediction for this frame
                        frame_index = prediction_data.get("frame_index")
                        predictions = prediction_data.get("predictions")
                        if frame_index is not None and predictions is not None:
                            _SCENARIOS[scenario_id]["predictions"][str(frame_index)] = predictions
                            logger.info(f"[API] Updated predictions for scenario {scenario_id} frame {frame_index}")

        except Exception as e:
            logger.error(f"[API] Error in update listener: {str(e)}")
            time.sleep(5)
            consumer = _connect_consumer()

# Start the update listener in a background thread
update_listener_thread = threading.Thread(target=_listen_for_updates, daemon=True)
update_listener_thread.start()

def create_scenario(scenario_id: str, video_path: str):
    if scenario_id in _SCENARIOS:
        raise ValueError("Scenario already exists")
    _SCENARIOS[scenario_id] = {
        "state": "init_startup",
        "video_path": video_path,
        "predictions": {}
    }

def update_scenario_state(scenario_id: str, new_state: str):
    if scenario_id not in _SCENARIOS:
        raise KeyError("Scenario not found")
    _SCENARIOS[scenario_id]["state"] = new_state

def get_scenario(scenario_id: str):
    return _SCENARIOS.get(scenario_id)

def set_predictions(scenario_id: str, predictions):
    if scenario_id not in _SCENARIOS:
        raise KeyError("Scenario not found")
    _SCENARIOS[scenario_id]["predictions"] = predictions

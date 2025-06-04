# Simple in-memory scenario state store

_SCENARIOS = {}

def create_scenario(scenario_id: str, video_path: str):
    if scenario_id in _SCENARIOS:
        raise ValueError("Scenario already exists")
    _SCENARIOS[scenario_id] = {
        "state": "init_startup",
        "video_path": video_path,
        "predictions": None
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

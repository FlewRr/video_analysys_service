from storage import SessionLocal, Scenario, OutboxEvent
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
 
_SCENARIOS = {}

def create_scenario(scenario_id: str, video_path: str):
    if scenario_id in _SCENARIOS:
        raise ValueError("Scenario already exists")
    _SCENARIOS[scenario_id] = {
        "state": "init_startup",
        "video_path": video_path,
        "predictions": None
    }

def update_scenario_state(event_payload: dict):
    scenario_id = event_payload.get("scenario_id")
    new_state = event_payload.get("new_state")
    
    session = SessionLocal()
    try:
        scenario = session.query(Scenario).get(scenario_id)
        if not scenario:
            scenario = Scenario(id=scenario_id, state=new_state)
            session.add(scenario)
        else:
            scenario.state = new_state
            scenario.updated_at = datetime.utcnow()

        # Add event to outbox
        outbox_event = OutboxEvent(
            event_type='scenario_state_changed',
            payload=event_payload
        )
        session.add(outbox_event)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def get_scenario(scenario_id: str):
    return _SCENARIOS.get(scenario_id)

def set_predictions(scenario_id: str, predictions):
    if scenario_id not in _SCENARIOS:
        raise KeyError("Scenario not found")
    _SCENARIOS[scenario_id]["predictions"] = predictions

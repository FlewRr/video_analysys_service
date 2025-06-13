from storage import SessionLocal, Scenario, OutboxEvent
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def create_scenario(scenario_id: str, video_path: str):
    session = SessionLocal()
    try:
        # Check if scenario already exists
        existing = session.query(Scenario).get(scenario_id)
        if existing:
            raise ValueError("Scenario already exists")
            
        # Create new scenario
        scenario = Scenario(
            id=scenario_id,
            state="init_startup",
            video_path=video_path,
            predictions=[]
        )
        session.add(scenario)
        session.commit()
        logger.info(f"[Scenarios] Created new scenario {scenario_id}")
    except Exception as e:
        session.rollback()
        logger.error(f"[Scenarios] Error creating scenario: {str(e)}")
        raise
    finally:
        session.close()

def update_scenario_state(event_payload: dict):
    scenario_id = event_payload.get("scenario_id")
    new_state = event_payload.get("new_state")
    
    if not scenario_id or not new_state:
        raise ValueError("scenario_id and new_state are required")
    
    session = SessionLocal()
    try:
        scenario = session.query(Scenario).get(scenario_id)
        if not scenario:
            scenario = Scenario(id=scenario_id, state=new_state)
            session.add(scenario)
        else:
            scenario.state = new_state
            scenario.updated_at = datetime.utcnow()

        # Add event to outbox with proper event type
        outbox_event = OutboxEvent(
            event_type='scenario_state_changed',
            payload={
                "scenario_id": scenario_id,
                "new_state": new_state,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        session.add(outbox_event)
        session.commit()
        logger.info(f"[Scenarios] Updated scenario {scenario_id} state to {new_state}")
    except Exception as e:
        session.rollback()
        logger.error(f"[Scenarios] Error updating scenario state: {str(e)}")
        raise e
    finally:
        session.close()

def set_predictions(scenario_id: str, predictions: dict):
    session = SessionLocal()
    try:
        scenario = session.query(Scenario).get(scenario_id)
        if not scenario:
            raise ValueError("Scenario not found")
            
        # Initialize predictions as a list if it doesn't exist
        if scenario.predictions is None:
            scenario.predictions = []
            
        # Add new prediction to the list
        if scenario.predictions is None:
            predictions_list = []
        else:
            predictions_list = scenario.predictions

        predictions_list.append(predictions)
        scenario.predictions = predictions_list

        scenario.updated_at = datetime.utcnow()
        session.commit()
        logger.info(f"[Scenarios] Added new prediction for scenario {scenario_id}")
    except Exception as e:
        session.rollback()
        logger.error(f"[Scenarios] Error setting predictions: {str(e)}")
        raise
    finally:
        session.close()

def get_scenario(scenario_id: str):
    session = SessionLocal()
    try:
        scenario = session.query(Scenario).get(scenario_id)
        if scenario:
            return {
                "state": scenario.state,
                "video_path": scenario.video_path,
                "predictions": scenario.predictions or {}
            }
        return None
    finally:
        session.close()

from fastapi import APIRouter, HTTPException
import os
import json
from models import ScenarioInitRequest, ScenarioStateChangeRequest, ScenarioStatusResponse, PredictionResponse
from scenario_state import create_scenario, get_scenario
from kafka_client import send_scenario_message

router = APIRouter()

ALLOWED_STATES = json.loads(os.getenv('ALLOWED_STATES'))

@router.post("/scenario/", status_code=201)
def init_scenario(request: ScenarioInitRequest):
    try:
        create_scenario(request.scenario_id, request.video_path)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Notify orchestrator to start scenario
    send_scenario_message({
        "type": "start",
        "scenario_id": request.scenario_id,
        "video_path": request.video_path
    })

    return {"message": "Scenario initialized", "scenario_id": request.scenario_id}

@router.post("/scenario/{scenario_id}/")
def change_scenario_state(scenario_id: str, request: ScenarioStateChangeRequest):
    if request.new_state not in ALLOWED_STATES:
        raise HTTPException(status_code=400, detail="Invalid state")

    scenario = get_scenario(scenario_id)
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")

    # Send state change request to orchestrator
    send_scenario_message({
        "type": "state_change_request",
        "scenario_id": scenario_id,
        "new_state": request.new_state
    })

    return {"message": "State change request sent", "scenario_id": scenario_id, "requested_state": request.new_state}

@router.get("/scenario/{scenario_id}/", response_model=ScenarioStatusResponse)
def get_scenario_status(scenario_id: str):
    scenario = get_scenario(scenario_id)
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")
    return ScenarioStatusResponse(scenario_id=scenario_id, state=scenario["state"])

@router.get("/prediction/{scenario_id}/", response_model=PredictionResponse)
def get_prediction(scenario_id: str):
    scenario = get_scenario(scenario_id)
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")

    return PredictionResponse(scenario_id=scenario_id, predictions=scenario["predictions"])

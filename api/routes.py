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

    return {"message": "Scenario initialized", "scenario_id": request.scenario_id}

@router.post("/scenario/{scenario_id}")
def control_scenario(scenario_id: str, request: ScenarioStateChangeRequest):
    if request.action not in ["start", "shutdown"]:
        raise HTTPException(status_code=400, detail="Only 'start' and 'shutdown' actions are allowed")
    
    scenario = get_scenario(scenario_id)
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")

    # Send command to orchestrator
    send_scenario_message({
        "type": request.action,
        "scenario_id": scenario_id
    })

    return {"message": f"Scenario {request.action} command sent", "scenario_id": scenario_id}

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

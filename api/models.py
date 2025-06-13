from pydantic import BaseModel
from typing import Optional, List, Dict, Any

class ScenarioInitRequest(BaseModel):
    scenario_id: str
    video_path: str

class ScenarioStateChangeRequest(BaseModel):
    new_state: str

class ScenarioStatusResponse(BaseModel):
    scenario_id: str
    state: str

class PredictionResponse(BaseModel):
    scenario_id: str
    predictions: Optional[List[Dict[str, Any]]]

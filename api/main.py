from api import engine, Base, get_db, Scenario, Prediction
import asyncio
import datetime
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from uuid import uuid4
from pydantic import BaseModel

import logging
logging.basicConfig(level=logging.INFO)

app = FastAPI()

@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

class ScenarioCreateResponse(BaseModel):
    scenario_id: str
    status: str

class ScenarioUpdateRequest(BaseModel):
    status: str

class PredictionCreateRequest(BaseModel):
    label: str
    confidence: str
    extra: str = None

@app.post("/scenario/", response_model=ScenarioCreateResponse)
async def create_scenario(db: AsyncSession = Depends(get_db)):
    scenario_id = str(uuid4())
    scenario = Scenario(id=scenario_id, status="init_startup")
    db.add(scenario)
    await db.commit()
    logging.info(f"Создан сценарий: {scenario_id}")
    return {"scenario_id": scenario_id, "status": scenario.status}

@app.post("/scenario/{scenario_id}/")
async def update_scenario(scenario_id: str, data: ScenarioUpdateRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Scenario).where(Scenario.id == scenario_id))
    scenario = result.scalar_one_or_none()
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")
    scenario.status = data.status
    await db.commit()
    return {"scenario_id": scenario_id, "status": scenario.status}

@app.get("/scenario/{scenario_id}/")
async def get_scenario(scenario_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Scenario).where(Scenario.id == scenario_id))
    scenario = result.scalar_one_or_none()
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")
    return {"scenario_id": scenario_id, "status": scenario.status}

@app.post("/prediction/{scenario_id}/")
async def post_prediction(scenario_id: str, data: PredictionCreateRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Scenario).where(Scenario.id == scenario_id))
    scenario = result.scalar_one_or_none()
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")

    prediction = Prediction(
        scenario_id=scenario_id,
        label=data.label,
        confidence=data.confidence,
        extra=data.extra,
    )
    db.add(prediction)
    await db.commit()
    return {"message": "Prediction saved", "scenario_id": scenario_id}

@app.get("/prediction/{scenario_id}/")
async def get_predictions(scenario_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Prediction).where(Prediction.scenario_id == scenario_id))
    predictions = result.scalars().all()
    return {
        "scenario_id": scenario_id,
        "predictions": [
            {"label": p.label, "confidence": p.confidence, "extra": p.extra} for p in predictions
        ],
    }

@app.post("/scenario/{scenario_id}/heartbeat")
async def heartbeat(scenario_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Scenario).where(Scenario.id == scenario_id))
    scenario = result.scalar_one_or_none()
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")
    scenario.last_heartbeat = datetime.utcnow()
    await db.commit()
    return {"message": "heartbeat updated"}
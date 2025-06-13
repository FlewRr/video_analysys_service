from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, JSON, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Connect to the existing database
logger.info("[Storage] Connecting to database at /db/db.sqlite")
engine = create_engine('sqlite:////db/db.sqlite', echo=False)  # Enable SQL logging
Base = declarative_base()

class Scenario(Base):
    __tablename__ = "scenarios"

    id = Column(String, primary_key=True)
    state = Column(String, nullable=False)
    video_path = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    predictions = Column(JSON)

class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id = Column(Integer, primary_key=True)
    event_type = Column(String, nullable=False)
    payload = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    processed = Column(Boolean, default=False)

# Create tables if they don't exist
logger.info("[Storage] Creating database tables if they don't exist...")
Base.metadata.create_all(engine)
logger.info("[Storage] Database tables created successfully")

SessionLocal = sessionmaker(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        logger.info("[Storage] Created new database session")
        yield db
    finally:
        logger.info("[Storage] Closing database session")
        db.close()

def create_scenario(db, scenario_id: str, video_path: str):
    scenario = Scenario(
        id=scenario_id,
        state="init_startup",
        video_path=video_path,
        predictions=[]
    )
    db.add(scenario)
    db.commit()
    return scenario

def get_scenario(db, scenario_id: str):
    # all_ids = [s.id for s in db.query(Scenario.id).all()]
    # logger.info(f"[DEBUG] Existing scenario IDs in DB: {all_ids}")
    # logger.info(f"[DEBUG] Trying to fetch scenario_id='{scenario_id}'")
    scenario = db.query(Scenario).filter(Scenario.id == scenario_id).first()
    # logger.info(f"[DEBUG] db query result: id={scenario.id}, predictions={scenario.predictions}")

    return scenario

def update_scenario_state(db, scenario_id: str, new_state: str):
    scenario = get_scenario(db, scenario_id)
    if scenario:
        scenario.state = new_state
        scenario.updated_at = datetime.utcnow()
        db.commit()
    return scenario

def update_scenario_predictions(db, scenario_id: str, predictions: dict):
    logger.info(f"[UPDATE SCENARIO PREDICTOPNS] GOT PREDICTIONS: {predictions}")
    scenario = get_scenario(db, scenario_id)
    logger.info(f"[SCENARIO] id={scenario.id}, predictions={scenario.predictions}, verdict={bool(scenario is not None)}")
    if scenario is not None:
        if scenario.predictions is None:
            scenario.predictions = []
        
        preds = scenario.predictions
        preds.append(predictions)
        
        scenario.predictions = preds
        scenario.updated_at = datetime.utcnow()

        db.commit()



import json
import logging
import os
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
import time
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

logger = logging.getLogger(__name__)

# Connect to the existing database
engine = create_engine('sqlite:////db/db.sqlite')
Base = declarative_base()

class Scenario(Base):
    __tablename__ = "scenarios"

    id = Column(String, primary_key=True)
    state = Column(String, nullable=False)
    video_path = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    predictions = Column(JSON)

# Create tables if they don't exist
logger.info("[API] Creating database tables if they don't exist...")
Base.metadata.create_all(engine)
logger.info("[API] Database tables created successfully")

SessionLocal = sessionmaker(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_scenario(scenario_id: str, video_path: str):
    db = SessionLocal()
    try:
        # Check if scenario already exists
        existing = db.query(Scenario).filter(Scenario.id == scenario_id).first()
        if existing:
            raise ValueError(f"Scenario with ID {scenario_id} already exists")
            
        scenario = Scenario(
            id=scenario_id,
            state="init_startup",
            video_path=video_path,
            predictions=[]
        )
        db.add(scenario)
        db.commit()
        return scenario
    except Exception as e:
        logger.error(f"[API] Error creating scenario: {str(e)}")
        db.rollback()
        raise
    finally:
        db.close()

def get_scenario(scenario_id: str):
    db = SessionLocal()
    try:
        scenario = db.query(Scenario).filter(Scenario.id == scenario_id).first()
        if scenario:
            return {
                "state": scenario.state,
                "video_path": scenario.video_path,
                "predictions": scenario.predictions or []
            }
        return None
    finally:
        db.close()


def _connect_consumer():
    max_retries = 30
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                os.getenv('SCENARIO_TOPIC'),
                os.getenv('PREDICTION_TOPIC'),
                bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
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

                if topic == os.getenv('SCENARIO_TOPIC') and message.get("type") == "state_change":
                    scenario_id = message.get("scenario_id")
                    new_state = message.get("new_state")
                    if scenario_id and new_state:
                        db = SessionLocal()
                        try:
                            scenario = db.query(Scenario).filter(Scenario.id == scenario_id).first()
                            if scenario:
                                scenario.state = new_state
                                scenario.updated_at = datetime.utcnow()
                                db.commit()
                                logger.info(f"[API] Updated scenario {scenario_id} state to {new_state}")
                            else:
                                logger.warning(f"[API] Received state change for unknown scenario {scenario_id}")
                        except Exception as e:
                            logger.error(f"[API] Error updating scenario state: {str(e)}")
                            db.rollback()
                        finally:
                            db.close()
        except Exception as e:
            logger.error(f"[API] Error in update listener: {str(e)}")
            time.sleep(5)
            consumer = _connect_consumer()

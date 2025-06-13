import json
import logging
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
import time
from kafka_producer import OrchestratorKafkaProducer
from storage import SessionLocal, OutboxEvent
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

# Define valid event types and their corresponding topics
EVENT_ROUTING = {
    'scenario_state_changed': os.getenv('SCENARIO_TOPIC'),
    'runner_command': os.getenv('RUNNER_TOPIC')
}

Base = declarative_base()

class Outbox(Base):
    __tablename__ = "outbox"

    id = Column(Integer, primary_key=True)
    event_type = Column(String, nullable=False)
    payload = Column(JSON, nullable=False)
    sent = Column(Boolean, default=False)
    sent_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)

engine = create_engine('sqlite:////db/db.sqlite')
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_outbox_event(event_type: str, payload: dict):
    db = SessionLocal()
    try:
        event = Outbox(
            event_type=event_type,
            payload=payload
        )
        db.add(event)
        db.commit()
        return event
    except Exception as e:
        logger.error(f"[ORCHESTRATOR] Error creating outbox event: {str(e)}")
        db.rollback()
        raise
    finally:
        db.close()

def get_unsent_events():
    db = SessionLocal()
    try:
        events = db.query(Outbox).filter(Outbox.sent == False).all()
        return events
    finally:
        db.close()

def mark_event_sent(event_id: int):
    db = SessionLocal()
    try:
        event = db.query(Outbox).filter(Outbox.id == event_id).first()
        if event:
            event.sent = True
            event.sent_at = datetime.utcnow()
            db.commit()
    except Exception as e:
        logger.error(f"[ORCHESTRATOR] Error marking event as sent: {str(e)}")
        db.rollback()
    finally:
        db.close()

def send_outbox_events():
    session = SessionLocal()
    try:
        # Get unsent events in batches of 100
        events = session.query(Outbox).filter_by(sent=False).limit(100).all()
        if not events:
            return

        producer = OrchestratorKafkaProducer.get_instance()
        
        for event in events:
            try:
                # Get the appropriate topic for this event type
                topic = EVENT_ROUTING.get(event.event_type)
                if not topic:
                    logger.error(f"[Outbox] Unknown event type: {event.event_type}")
                    event.sent = True  # Mark as sent to prevent retrying invalid events
                    event.sent_at = datetime.utcnow()
                    continue

                # Send message to appropriate Kafka topic
                if topic == os.getenv('RUNNER_TOPIC'):
                    producer.send_runner_command(event.payload)
                else:
                    # For other topics, use the producer directly
                    producer.producer.send(topic, event.payload)
                
                producer.producer.flush()

                # Mark event as sent
                event.sent = True
                event.sent_at = datetime.utcnow()
                session.commit()
                logger.info(f"[Outbox] Successfully sent event: {event.id} of type {event.event_type}")
            except Exception as e:
                logger.error(f"[Outbox] Error sending event {event.id}: {str(e)}")
                session.rollback()
                # Don't re-raise the exception to continue processing other events
                continue
    except Exception as e:
        logger.error(f"[Outbox] Error in send_outbox_events: {str(e)}")
        session.rollback()
    finally:
        session.close()

def outbox_poller_loop():
    logger.info("[Outbox] Starting outbox poller loop")
    while True:
        try:
            send_outbox_events()
        except Exception as e:
            logger.error(f"[Outbox] Error in poller loop: {str(e)}")
        time.sleep(5)  # polling interval

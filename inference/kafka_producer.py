import json
import time
import logging
import os
from kafka import KafkaProducer as KafkaClient
from kafka.errors import NoBrokersAvailable, KafkaError
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import threading

logger = logging.getLogger(__name__)

# Outbox DB setup
engine = create_engine('sqlite:///inference.db', echo=False)
Base = declarative_base()

class OutboxEvent(Base):
    __tablename__ = 'outbox_events'
    id = Column(Integer, primary_key=True)
    event_type = Column(String, nullable=False)
    payload = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    processed = Column(Boolean, default=False)

Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)

def create_outbox_event(event_type: str, payload: dict):
    db = SessionLocal()
    try:
        event = OutboxEvent(event_type=event_type, payload=payload)
        db.add(event)
        db.commit()
        return event
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()

def get_unsent_events():
    db = SessionLocal()
    try:
        return db.query(OutboxEvent).filter(OutboxEvent.processed == False).all()
    finally:
        db.close()

def mark_event_sent(event_id: int):
    db = SessionLocal()
    try:
        event = db.query(OutboxEvent).filter(OutboxEvent.id == event_id).first()
        if event:
            event.processed = True
            db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

# Add poller to send outbox events
def outbox_poller_loop():
    producer = InferenceKafkaProducer.get_instance()
    while True:
        events = get_unsent_events()
        for event in events:
            try:
                if event.event_type == 'prediction_ready':
                    topic = os.getenv('PREDICTION_TOPIC')
                    producer.producer.send(topic, {
                        'event_type': event.event_type,
                        'payload': event.payload
                    })
                    producer.producer.flush()
                    mark_event_sent(event.id)
            except Exception as e:
                continue
        time.sleep(5)

# Add function to be called when prediction is ready
def notify_prediction_ready(scenario_id: str):
    payload = {
        'scenario_id': scenario_id,
        'timestamp': datetime.utcnow().isoformat()
    }
    create_outbox_event('prediction_ready', payload)

class InferenceKafkaProducer:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(InferenceKafkaProducer, cls).__new__(cls)
            cls._instance.producer = None
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self._connect_producer()

    def _connect_producer(self):
        max_retries = 30
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"[Producer] Connecting to Kafka at {os.getenv('KAFKA_BOOTSTRAP_SERVERS')} (attempt {attempt + 1}/{max_retries})")
                self.producer = KafkaClient(
                    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3
                )
                logger.info("[Producer] Successfully connected to Kafka")
                return
            except (NoBrokersAvailable, KafkaError) as e:
                logger.warning(f"[Producer] Kafka not available yet: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Could not connect to Kafka after {max_retries} attempts")

    def send_prediction(self, scenario_id: str, predictions: dict):
        """Send predictions to Kafka"""
        try:
            if not self.producer:
                logger.error("[Producer] Producer not initialized")
                self._connect_producer()
            
            message = {
                "scenario_id": scenario_id,
                "predictions": predictions
            }
            logger.info(f"[Producer] Sending prediction message: {message}")
            
            # Send message and wait for acknowledgment
            future = self.producer.send(os.getenv('PREDICTION_TOPIC'), message)
            # Wait for the message to be delivered
            future.get(timeout=10)
            # Flush to ensure message is sent
            self.producer.flush()
            
            logger.info(f"[Producer] Successfully sent prediction for scenario {scenario_id} to topic {os.getenv('PREDICTION_TOPIC')}")
        except Exception as e:
            logger.error(f"[Producer] Error sending prediction: {str(e)}")
            raise

    def close(self):
        if self.producer:
            try:
                self.producer.flush()
                self.producer.close()
                self.producer = None
                logger.info("[Producer] Successfully closed producer")
            except Exception as e:
                logger.error(f"[Producer] Error closing producer: {str(e)}")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

# Export the send_prediction function for backward compatibility
def send_prediction(scenario_id: str, predictions: dict):
    return InferenceKafkaProducer.get_instance().send_prediction(scenario_id, predictions)

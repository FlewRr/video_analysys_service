import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
from storage import SessionLocal, OutboxEvent
from datetime import datetime
from config import KAFKA_BOOTSTRAP_SERVERS
from kafka_producer import producer

def send_outbox_events():
    session = SessionLocal()
    try:
        events = session.query(OutboxEvent).filter_by(sent=False).all()
        for event in events:
            # Send message to Kafka topic
            producer.send('scenario_updates', value=event.payload)
            producer.flush()
            # Mark event as sent
            event.sent = True
            event.sent_at = datetime.utcnow()
            session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error sending outbox events: {e}")
    finally:
        session.close()
        
def outbox_poller_loop():
    while True:
        try:
            send_outbox_events()
        except Exception as e:
            print(f"Error in outbox poller: {e}")
        time.sleep(5)  # adjust polling interval if needed

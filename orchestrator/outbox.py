import time
from kafka_producer import producer
from storage import SessionLocal, OutboxEvent
from datetime import datetime

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
        print(f"[Outbox] Error sending outbox events: {e}")
    finally:
        session.close()

def outbox_poller_loop():
    while True:
        try:
            send_outbox_events()
        except Exception as e:
            print(f"[Outbox] Error in poller loop: {e}")
        time.sleep(5)  # polling interval

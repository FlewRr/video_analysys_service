import time
from kafka_producer import OrchestratorKafkaProducer
from storage import SessionLocal, OutboxEvent
from datetime import datetime
import logging
from config import RUNNER_TOPIC, SCENARIO_TOPIC

logger = logging.getLogger(__name__)

# Define valid event types and their corresponding topics
EVENT_ROUTING = {
    'scenario_state_changed': SCENARIO_TOPIC,
    'runner_command': RUNNER_TOPIC
}

def send_outbox_events():
    session = SessionLocal()
    try:
        # Get unsent events in batches of 100
        events = session.query(OutboxEvent).filter_by(sent=False).limit(100).all()
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
                if topic == RUNNER_TOPIC:
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

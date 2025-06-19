from fastapi import FastAPI
from contextlib import asynccontextmanager
from kafka_consumer import KafkaListener
from kafka_producer import OrchestratorKafkaProducer
from storage import Base, engine  # Import storage to ensure tables are created
from heartbeat import heartbeat_monitor
import threading
import logging
import signal
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ensure database tables exist
Base.metadata.create_all(engine)

# Global variables to store thread references
kafka_thread = None

def handle_shutdown(signum, frame):
    logger.info("[Orchestrator] Received shutdown signal")
    if kafka_thread and kafka_thread.is_alive():
        kafka_thread.join(timeout=5)
    heartbeat_monitor.stop()
    OrchestratorKafkaProducer.get_instance().close()

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_thread
    
    logger.info("[Orchestrator] Starting background tasks...")
    
    # Initialize and start Kafka listener
    listener = KafkaListener()
    kafka_thread = threading.Thread(target=listener.listen, daemon=True)
    kafka_thread.start()
    logger.info("[Orchestrator] Kafka listener thread started")
    
    # Start heartbeat monitor
    heartbeat_monitor.start()
    logger.info("[Orchestrator] Heartbeat monitor started")
    
    try:
        yield
    finally:
        logger.info("[Orchestrator] Shutting down...")
        if kafka_thread and kafka_thread.is_alive():
            kafka_thread.join(timeout=5)
        heartbeat_monitor.stop()
        OrchestratorKafkaProducer.get_instance().close()

app = FastAPI(title="Orchestrator", lifespan=lifespan)

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "kafka_thread_alive": kafka_thread.is_alive() if kafka_thread else False,
        "heartbeat_monitor_alive": heartbeat_monitor.monitor_thread.is_alive() if heartbeat_monitor.monitor_thread else False
    }


import os

db_path = os.path.abspath('/db/db.sqlite')
logger.info(f"[DEBUG] Using SQLite DB at {db_path}")
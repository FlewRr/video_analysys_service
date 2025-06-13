from fastapi import FastAPI
from contextlib import asynccontextmanager
from routes import router
from kafka_client import KafkaClient
import logging
import signal
from scenario_state import _listen_for_updates
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def handle_shutdown(signum, frame):
    logger.info("[API] Received shutdown signal")
    KafkaClient.get_instance().close()

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[API] Starting up...")

    update_listener_thread = threading.Thread(target=_listen_for_updates, daemon=True)
    update_listener_thread.start()

    try:
        yield
    finally:
        logger.info("[API] Shutting down...")
        KafkaClient.get_instance().close()

app = FastAPI(title="Video Analysis API", lifespan=lifespan)
app.include_router(router)

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "kafka_connected": KafkaClient.get_instance().producer is not None
    }

import os

db_path = os.path.abspath('/db/db.sqlite')
logger.info(f"[DEBUG] Using SQLite DB at {db_path}")
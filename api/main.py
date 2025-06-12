from fastapi import FastAPI
from contextlib import asynccontextmanager
from routes import router
from kafka_client import KafkaClient
import logging
import signal

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
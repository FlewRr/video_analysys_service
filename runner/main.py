from fastapi import FastAPI
from contextlib import asynccontextmanager
from kafka_listener import KafkaListener
import threading
import logging
import signal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variable to store thread reference
kafka_thread = None

def handle_shutdown(signum, frame):
    logger.info("[Runner] Received shutdown signal")
    if kafka_thread and kafka_thread.is_alive():
        KafkaListener.get_instance().stop()
        kafka_thread.join(timeout=5)

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_thread
    
    logger.info("[Runner] Starting up...")
    
    # Initialize and start Kafka listener
    listener = KafkaListener.get_instance()
    kafka_thread = threading.Thread(target=listener.listen, daemon=True)
    kafka_thread.start()
    logger.info("[Runner] Kafka listener thread started")
    
    try:
        yield
    finally:
        logger.info("[Runner] Shutting down...")
        if kafka_thread and kafka_thread.is_alive():
            listener.stop()
            kafka_thread.join(timeout=5)

app = FastAPI(title="Video Analysis Runner", lifespan=lifespan)

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "kafka_thread_alive": kafka_thread.is_alive() if kafka_thread else False
    }

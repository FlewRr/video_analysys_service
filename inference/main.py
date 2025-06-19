from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from kafka_consumer import InferenceKafkaConsumer, SessionLocal, Prediction
from kafka_producer import InferenceKafkaProducer, outbox_poller_loop
import threading
import logging
import signal
from yolo import YoloNano
import sqlalchemy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables
kafka_thread = None
yolo_model = None

def handle_shutdown(signum, frame):
    logger.info("[Inference] Received shutdown signal")
    if kafka_thread and kafka_thread.is_alive():
        InferenceKafkaConsumer.get_instance().stop()
        kafka_thread.join(timeout=5)
    InferenceKafkaProducer.get_instance().close()

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_thread, yolo_model
    
    logger.info("[Inference] Starting up...")
    
    try:
        yolo_model = YoloNano(device='cpu')
        logger.info("[Inference] YOLO model initialized")
    except Exception as e:
        logger.error(f"[Inference] Error initializing YOLO model: {str(e)}")
        raise
    
    consumer = InferenceKafkaConsumer.get_instance()
    kafka_thread = threading.Thread(target=consumer.listen, daemon=True)
    kafka_thread.start()
    logger.info("[Inference] Kafka consumer thread started")

    # Start outbox poller
    outbox_thread = threading.Thread(target=outbox_poller_loop, daemon=True)
    outbox_thread.start()
    logger.info("[Inference] Outbox poller thread started")
    
    try:
        yield
    finally:
        logger.info("[Inference] Shutting down...")
        if kafka_thread and kafka_thread.is_alive():
            consumer.stop()
            kafka_thread.join(timeout=5)
        InferenceKafkaProducer.get_instance().close()

app = FastAPI(title="Video Analysis Inference", lifespan=lifespan)

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "kafka_thread_alive": kafka_thread.is_alive() if kafka_thread else False,
        "yolo_model_initialized": yolo_model is not None
    }

@app.get("/predictions/{scenario_id}")
def get_predictions(scenario_id: str):
    db = SessionLocal()
    try:
        preds = db.query(Prediction).filter(Prediction.scenario_id == scenario_id).order_by(Prediction.frame_index).all()
        if not preds:
            return {"predictions": []}
        return {"predictions": [
            {
                "frame_index": p.frame_index,
                "predictions": p.predictions,
                "created_at": p.created_at.isoformat()
            } for p in preds
        ]}
    except sqlalchemy.exc.SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

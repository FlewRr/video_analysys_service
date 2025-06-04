import threading
from fastapi import FastAPI
from inference.kafka_consumer import listen

app = FastAPI(title="Inference Service")

@app.get("/health")
def health():
    return {"status": "inference alive"}

# Start Kafka consumer in background thread
threading.Thread(target=listen, daemon=True).start()

#uvicorn inference.main:app --host 0.0.0.0 --port 8003

import threading
import time
from fastapi import FastAPI
from kafka_consumer import listen
from outbox import outbox_poller_loop

app = FastAPI(title="Orchestrator")

@app.get("/health")
def health():
    return {"status": "orchestrator alive"}


# Start Kafka consumer in background thread
threading.Thread(target=listen, daemon=True).start()

# Start outbox poller in background thread
threading.Thread(target=outbox_poller_loop, daemon=True).start()

# uvicorn main:app --host 0.0.0.0 --port 8001
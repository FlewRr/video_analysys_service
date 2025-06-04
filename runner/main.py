from fastapi import FastAPI
import threading
from kafka_listener import listen

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "runner alive"}

# Start Kafka listener in background
threading.Thread(target=listen, daemon=True).start()


# uvicorn runner.main:app --host 0.0.0.0 --port 8002
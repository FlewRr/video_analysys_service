import threading
import time
from fastapi import FastAPI
from orchestrator.kafka_consumer import listen
from orchestrator.outbox import send_outbox_events  # Assuming you put the function here

app = FastAPI(title="Orchestrator")

@app.get("/health")
def health():
    return {"status": "orchestrator alive"}

def outbox_poller_loop():
    while True:
        try:
            send_outbox_events()
        except Exception as e:
            print(f"Error in outbox poller: {e}")
        time.sleep(5)  # adjust polling interval if needed

# Start Kafka consumer in background thread
threading.Thread(target=listen, daemon=True).start()

# Start outbox poller in background thread
threading.Thread(target=outbox_poller_loop, daemon=True).start()

# uvicorn orchestrator.main:app --host 0.0.0.0 --port 8001
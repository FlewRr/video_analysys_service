from fastapi import FastAPI
from contextlib import asynccontextmanager
from kafka_consumer import KafkaListener
from outbox import outbox_poller_loop
import threading

print("[Orchestrator] module load")

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[Orchestrator] Starting background tasks...")
    listener = KafkaListener()
    threading.Thread(target=listener.listen, daemon=True).start()
    threading.Thread(target=outbox_poller_loop, daemon=True).start()
    yield
    print("[Orchestrator] Shutdown...")

app = FastAPI(title="Orchestrator", lifespan=lifespan)

# @app.get("/health")
# def health():
#     return {"status": "orchestrator alive"}

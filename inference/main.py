import threading
from fastapi import FastAPI
from kafka_consumer import listen
from contextlib import asynccontextmanager
print("[Inference] module load")
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[Inference] Starting background tasks...")
    threading.Thread(target=listen, daemon=True).start()
    yield
    print("[Inference] Shutdown...")

app = FastAPI(title="Inference", lifespan=lifespan)

@app.get("/health")
def health():
    return {"status": "inference alive"}

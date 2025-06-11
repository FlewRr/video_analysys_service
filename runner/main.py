from fastapi import FastAPI
import threading
from contextlib import asynccontextmanager
from kafka_listener import listen

print("[Runner] module load")
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[Runner] Starting background tasks...")
    threading.Thread(target=listen, daemon=True).start()
    yield
    print("[Runner] Shutdown...")

app = FastAPI(title="Runner", lifespan=lifespan)

@app.get("/health")
def health():
    return {"status": "runner alive"}

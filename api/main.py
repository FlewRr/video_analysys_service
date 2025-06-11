from fastapi import FastAPI
from routes import router

app = FastAPI(title="Video Analysis API")

app.include_router(router)

@app.get("/health")
def health():
    return {"status": "api alive"}
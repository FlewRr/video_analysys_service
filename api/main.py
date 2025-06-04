from fastapi import FastAPI
from api.routes import router

app = FastAPI(title="Video Analysis API")

app.include_router(router)

# uvicorn api.main:app --host 0.0.0.0 --port 8000

# curl -X POST http://localhost:8000/scenario/ -H "Content-Type: application/json" -d '{"video_path":"path/to/test_video.mp4"}'
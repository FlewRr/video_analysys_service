# runner/main.py
import uvicorn
from fastapi import FastAPI, BackgroundTasks, UploadFile, File, HTTPException
import os
import shutil
from processor import process_video
import requests
import time

def send_heartbeat(scenario_id):
    while True:
        try:
            requests.post(f"http://api:8000/scenario/{scenario_id}/heartbeat")
        except:
            pass
        time.sleep(10)

app = FastAPI()

@app.post("/start-processing/")
async def start_video_processing(scenario_id: str, background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    # Проверка, что scenario_id валиден, можно добавить вызов API сценариев, если нужно

    tmp_dir = "tmp_videos"
    os.makedirs(tmp_dir, exist_ok=True)
    video_path = file.filename

    with open(video_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    background_tasks.add_task(process_video, video_path, scenario_id)

    return {"message": "Processing started", "scenario_id": scenario_id, "video_path": video_path}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8004)

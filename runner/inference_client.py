import requests
from runner.config import INFERENCE_URL

def send_to_inference(scenario_id: str, frames: list):
    results = []
    for idx, frame_bytes in enumerate(frames):
        try:
            files = {'frame': ('frame.jpg', frame_bytes, 'image/jpeg')}
            response = requests.post(INFERENCE_URL, files=files)
            prediction = response.json()
            results.append({
                "frame_index": idx,
                "prediction": prediction
            })
        except Exception as e:
            results.append({
                "frame_index": idx,
                "error": str(e)
            })
    return results

from typing import List

import torch
from fastapi import FastAPI, File, UploadFile
from transformers import AutoProcessor, AutoModelForCausalLM
from PIL import Image
import numpy as np
import cv2

app = FastAPI()

processor = AutoProcessor.from_pretrained("microsoft/git-base")
model = AutoModelForCausalLM.from_pretrained("microsoft/git-base")
model.eval()

@app.post("/inference/{scenario_id}/")
async def inference(scenario_id: str, files: List[UploadFile] = File(...)):
    images = []
    for file in files:
        contents = await file.read()
        nparr = np.frombuffer(contents, np.uint8)
        img_bgr = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        img_rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)
        pil_img = Image.fromarray(img_rgb)

        images.append(pil_img)

    if not images:
        return {"scenario_id": scenario_id, "prediction": "No images provided"}


    inputs = processor(images=images, return_tensors="pt")
    with torch.no_grad():
        generated_ids = model.generate(pixel_values=inputs["pixel_values"], max_length=50)
    caption = processor.batch_decode(generated_ids, skip_special_tokens=True)[0]

    return {
        "scenario_id": scenario_id,
        "prediction": caption
    }
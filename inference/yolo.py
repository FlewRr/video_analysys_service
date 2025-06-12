from ultralytics import YOLO
import cv2
import numpy as np
from typing import List, Dict, Any
import logging
import os

logger = logging.getLogger(__name__)

class YoloNano:
    def __init__(self, device='cpu'):
        self.device = device
        self.model = YOLO('yolov8n.pt').to(self.device)

        logger.info(f"[YoloNano] Initialized with device: {device}")

    def predict(self, frame: np.ndarray) -> List[Dict[str, Any]]:
        try:
            frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)

            results = self.model.predict(frame_rgb, device=self.device, conf=0.1)
            detections = []
            for r in results:
                for box in r.boxes:
                    x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())
                    conf = float(box.conf[0])
                    cls_id = int(box.cls[0])
                    detections.append({
                        "class": self.model.names[cls_id],
                        "confidence": conf,
                        "bbox": [x1, y1, x2, y2]
                    })
            logger.info(f"[YoloNano] Found {len(detections)} objects in frame")
            return detections
        except Exception as e:
            logger.error(f"[YoloNano] Error in predict: {str(e)}")
            raise
            
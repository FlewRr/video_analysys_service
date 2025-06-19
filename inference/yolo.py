from ultralytics import YOLO
import cv2
import numpy as np
from typing import List, Dict, Any
import logging
import os

logger = logging.getLogger(__name__)

class YoloNano:
    _instance = None
    _model = None

    def __new__(cls, device='cpu'):
        if cls._instance is None:
            cls._instance = super(YoloNano, cls).__new__(cls)
            cls._instance.device = device
            cls._instance._initialize_model()
        return cls._instance

    def _initialize_model(self):
        if YoloNano._model is None:
            logger.info(f"[YoloNano] Initializing model on device: {self.device}")
            YoloNano._model = YOLO('yolov8n.pt').to(self.device)
            logger.info("[YoloNano] Model initialized successfully")

    def predict(self, frame: np.ndarray) -> List[Dict[str, Any]]:
        try:
            # Ensure frame is in RGB format
            if len(frame.shape) == 3 and frame.shape[2] == 3:
                if frame.dtype != np.uint8:
                    frame = (frame * 255).astype(np.uint8)
            else:
                raise ValueError(f"Invalid frame format: shape={frame.shape}, dtype={frame.dtype}")

            # Run prediction
            results = YoloNano._model.predict(frame, device=self.device, conf=0.1)
            
            # Process results
            detections = []
            for r in results:
                for box in r.boxes:
                    x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())
                    conf = float(box.conf[0])
                    cls_id = int(box.cls[0])
                    detections.append({
                        "class": YoloNano._model.names[cls_id],
                        "confidence": conf,
                        "bbox": [x1, y1, x2, y2]
                    })
            
            logger.info(f"[YoloNano] Found {len(detections)} objects in frame")
            return detections
            
        except Exception as e:
            logger.error(f"[YoloNano] Error in predict: {str(e)}")
            raise
            
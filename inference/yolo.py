# from ultralytics import YOLO

class YoloNano:
    def __init__(self, device='cpu'):
        self.device = device
        # Load YOLOv8n (Nano model) from Ultralytics
        # self.model = YOLO('yolov8n.pt')
        # self.model.to(self.device)

    def predict(self, frame):
        """
        frame: numpy array in RGB format (e.g., from OpenCV or PIL converted to np.array)
        """
        # results = self.model.predict(frame, device=self.device, verbose=False)
        # detections = []
        # for r in results:
        #     for box in r.boxes:
        #         x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())
        #         conf = float(box.conf[0])
        #         cls_id = int(box.cls[0])
        #         detections.append({
        #             "class": self.model.names[cls_id],
        #             "confidence": conf,
        #             "bbox": [x1, y1, x2, y2]
                # })
        
        detections = [{"class" : 1, "confidence": 0.9, "bbox": [0.1, 0.2, 0.3, 0.4]}, {"class" : 2, "confidence": 0.9, "bbox": [0.1, 0.2, 0.3, 0.4]}]
        return detections

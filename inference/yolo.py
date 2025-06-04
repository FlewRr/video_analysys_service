import torch

class YoloNano:
    def __init__(self, device='cpu'):
        self.device = device
        # Load pretrained YOLOv5 Nano model from Ultralytics hub
        self.model = torch.hub.load('ultralytics/yolov5', 'yolov5n', pretrained=True)
        self.model.to(self.device)
        self.model.eval()

    def predict(self, frame):
        """
        frame: numpy array in RGB format (from runner)
        """
        results = self.model(frame)
        detections = []
        for *box, conf, cls in results.xyxy[0].cpu().numpy():
            x1, y1, x2, y2 = map(int, box)
            detections.append({
                "class": self.model.names[int(cls)],
                "confidence": float(conf),
                "bbox": [x1, y1, x2, y2]
            })
        return detections

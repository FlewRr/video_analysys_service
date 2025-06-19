FROM python:3.10-slim

RUN apt-get update && apt-get install -y libgl1 libglib2.0-0

WORKDIR /inference

# Ensure the directory for the SQLite database exists and is writable
RUN mkdir -p /inference_db && chmod 777 /inference_db
ENV INFERENCE_DB_PATH=/inference_db/inference.db

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8003", "--lifespan=on"]
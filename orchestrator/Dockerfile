FROM python:3.10-slim

WORKDIR /orchestrator

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "main:app", "--reload", "--host", "0.0.0.0", "--port", "8001", "--lifespan=on"]
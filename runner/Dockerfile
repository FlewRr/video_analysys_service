FROM python:3.10-slim

RUN apt-get update && apt-get install -y libgl1 libglib2.0-0

WORKDIR /runner

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002", "--lifespan=on"]
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY batch-service/ batch-service/

CMD ["python", "batch-service/firestore-to_bigquery-batch-service.py"]

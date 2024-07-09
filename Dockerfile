FROM python:3.9-slim

WORKDIR /batch-service

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY batch-service/ batch-service/

CMD ["python", "batch-service/firestore-to-bigquery-batch-service.py"]

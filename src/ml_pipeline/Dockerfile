FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/common /app/common
COPY src/ml_pipeline /app/ml_pipeline

ENV PYTHONPATH=/app

EXPOSE 8002

CMD ["uvicorn", "ml_pipeline.pipeline:app", "--host", "0.0.0.0", "--port", "8002"]

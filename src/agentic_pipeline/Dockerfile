FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/common /app/common
COPY src/agentic_pipeline /app/agentic_pipeline

ENV PYTHONPATH=/app

EXPOSE 8003

CMD ["uvicorn", "agentic_pipeline.agent:app", "--host", "0.0.0.0", "--port", "8003"]

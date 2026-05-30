FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/common /app/common
COPY src/genai_pipeline /app/genai_pipeline

ENV PYTHONPATH=/app

EXPOSE 8001

CMD ["uvicorn", "genai_pipeline.groq_pipeline:app", "--host", "0.0.0.0", "--port", "8001"]

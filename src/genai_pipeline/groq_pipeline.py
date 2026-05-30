"""
GenAI Pipeline — Groq LLM Inference Service

Demonstrates production observability for an LLM-powered service:
  - Prometheus metrics (latency, tokens, errors, rate limits)
  - Structured JSON logs shipped to Loki / Elasticsearch
  - Distributed traces via OpenTelemetry → Jaeger
  - Kafka events for downstream consumers

Run standalone:
  GROQ_API_KEY=gsk_... uvicorn groq_pipeline:app --port 8001
"""

import os
import time
import asyncio
import random
import sys
from pathlib import Path

# Add common to path when running inside container
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel
from typing import Optional
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
import groq
from kafka import KafkaProducer
import json
import structlog

from common.metrics import (
    GENAI_REQUESTS, GENAI_REQUEST_DURATION, GENAI_TOKENS,
    GENAI_RATE_LIMIT_HITS, GENAI_CONTEXT_UTILIZATION, GENAI_QUEUE_DEPTH,
)
from common.logging_config import configure_logging, get_logger
from common.tracing import configure_tracing
from common.gcs_exporter import GCSExporter

# ── Bootstrap ────────────────────────────────────────────────────
SERVICE_NAME = os.getenv("SERVICE_NAME", "genai-pipeline")
configure_logging(SERVICE_NAME, os.getenv("LOG_LEVEL", "INFO"))
tracer = configure_tracing(SERVICE_NAME)
logger = get_logger(__name__)

app = FastAPI(title="GenAI Pipeline", version="1.0.0")

# ── Groq client ──────────────────────────────────────────────────
_groq_client: Optional[groq.Groq] = None

def get_groq_client() -> groq.Groq:
    global _groq_client
    if _groq_client is None:
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key or api_key == "your_groq_api_key_here":
            raise RuntimeError("GROQ_API_KEY not set. Get a free key at https://console.groq.com")
        _groq_client = groq.Groq(api_key=api_key)
    return _groq_client

# ── Kafka producer (fire-and-forget events) ──────────────────────
_kafka_producer: Optional[KafkaProducer] = None

def get_kafka_producer() -> Optional[KafkaProducer]:
    global _kafka_producer
    if _kafka_producer is None:
        try:
            _kafka_producer = KafkaProducer(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=3000,
                retries=2,
            )
        except Exception as e:
            logger.warning("kafka_unavailable", error=str(e))
    return _kafka_producer


def emit_kafka_event(topic: str, event: dict) -> None:
    producer = get_kafka_producer()
    if producer:
        try:
            producer.send(topic, event)
        except Exception as e:
            logger.warning("kafka_send_failed", topic=topic, error=str(e))


# ── Request / Response models ────────────────────────────────────
class InferenceRequest(BaseModel):
    prompt: str
    model: str = "llama-3.1-8b-instant"  # fast Groq model
    max_tokens: int = 512
    temperature: float = 0.7
    system_prompt: Optional[str] = "You are a helpful AI assistant."
    request_id: Optional[str] = None


class InferenceResponse(BaseModel):
    request_id: str
    model: str
    response: str
    input_tokens: int
    output_tokens: int
    latency_ms: float
    finish_reason: str


# ── Core inference function ──────────────────────────────────────
async def run_llm_inference(req: InferenceRequest) -> InferenceResponse:
    """Call Groq API with full observability instrumentation."""

    request_id = req.request_id or f"req-{int(time.time() * 1000)}"
    start_time = time.time()

    # Context window utilization estimate
    estimated_tokens = len(req.prompt.split()) + len((req.system_prompt or "").split())
    model_max_tokens = {"llama-3.1-8b-instant": 131072, "llama-3.3-70b-versatile": 131072,
                        "llama-3.1-70b-versatile": 131072, "gemma2-9b-it": 8192}
    max_ctx = model_max_tokens.get(req.model, 8192)
    ctx_utilization = min(estimated_tokens / max_ctx, 1.0)
    GENAI_CONTEXT_UTILIZATION.labels(service=SERVICE_NAME, model=req.model).set(ctx_utilization)

    GENAI_QUEUE_DEPTH.labels(service=SERVICE_NAME).inc()

    with tracer.start_as_current_span("llm_inference") as span:
        span.set_attribute("llm.model", req.model)
        span.set_attribute("llm.max_tokens", req.max_tokens)
        span.set_attribute("llm.temperature", req.temperature)
        span.set_attribute("request.id", request_id)

        try:
            client = get_groq_client()

            response = client.chat.completions.create(
                model=req.model,
                messages=[
                    {"role": "system", "content": req.system_prompt},
                    {"role": "user", "content": req.prompt},
                ],
                max_tokens=req.max_tokens,
                temperature=req.temperature,
            )

            latency_ms = (time.time() - start_time) * 1000
            usage = response.usage
            content = response.choices[0].message.content
            finish_reason = response.choices[0].finish_reason

            # ── Record metrics ────────────────────────────────────
            GENAI_REQUESTS.labels(
                service=SERVICE_NAME, model=req.model, status="success"
            ).inc()
            GENAI_REQUEST_DURATION.labels(
                service=SERVICE_NAME, model=req.model
            ).observe(latency_ms / 1000)
            GENAI_TOKENS.labels(
                service=SERVICE_NAME, model=req.model, type="input"
            ).inc(usage.prompt_tokens)
            GENAI_TOKENS.labels(
                service=SERVICE_NAME, model=req.model, type="output"
            ).inc(usage.completion_tokens)

            # ── Structured log ────────────────────────────────────
            logger.info(
                "llm_inference_success",
                request_id=request_id,
                model=req.model,
                input_tokens=usage.prompt_tokens,
                output_tokens=usage.completion_tokens,
                latency_ms=round(latency_ms, 2),
                finish_reason=finish_reason,
                ctx_utilization=round(ctx_utilization, 3),
            )

            # ── OTel span attributes ──────────────────────────────
            span.set_attribute("llm.input_tokens", usage.prompt_tokens)
            span.set_attribute("llm.output_tokens", usage.completion_tokens)
            span.set_attribute("llm.latency_ms", latency_ms)
            span.set_attribute("llm.finish_reason", finish_reason)
            span.set_status(Status(StatusCode.OK))

            # ── Kafka event (downstream consumers) ───────────────
            emit_kafka_event("genai-events", {
                "event_type": "inference_completed",
                "request_id": request_id,
                "model": req.model,
                "input_tokens": usage.prompt_tokens,
                "output_tokens": usage.completion_tokens,
                "latency_ms": round(latency_ms, 2),
                "timestamp": time.time(),
            })

            return InferenceResponse(
                request_id=request_id,
                model=req.model,
                response=content,
                input_tokens=usage.prompt_tokens,
                output_tokens=usage.completion_tokens,
                latency_ms=round(latency_ms, 2),
                finish_reason=finish_reason,
            )

        except groq.RateLimitError as e:
            latency_ms = (time.time() - start_time) * 1000
            GENAI_REQUESTS.labels(
                service=SERVICE_NAME, model=req.model, status="rate_limited"
            ).inc()
            GENAI_RATE_LIMIT_HITS.labels(service=SERVICE_NAME, model=req.model).inc()
            span.set_status(Status(StatusCode.ERROR, str(e)))

            logger.warning(
                "llm_rate_limited",
                request_id=request_id,
                model=req.model,
                latency_ms=round(latency_ms, 2),
                error=str(e),
            )
            raise HTTPException(status_code=429, detail="LLM rate limit reached. Retry after backoff.")

        except groq.APIError as e:
            latency_ms = (time.time() - start_time) * 1000
            GENAI_REQUESTS.labels(
                service=SERVICE_NAME, model=req.model, status="error"
            ).inc()
            span.set_status(Status(StatusCode.ERROR, str(e)))

            logger.error(
                "llm_api_error",
                request_id=request_id,
                model=req.model,
                latency_ms=round(latency_ms, 2),
                error_type=type(e).__name__,
                error=str(e),
            )
            raise HTTPException(status_code=502, detail=f"LLM API error: {e}")

        finally:
            GENAI_QUEUE_DEPTH.labels(service=SERVICE_NAME).dec()


# ── FastAPI Routes ───────────────────────────────────────────────
@app.post("/infer", response_model=InferenceResponse)
async def infer(req: InferenceRequest):
    """Run LLM inference via Groq API."""
    return await run_llm_inference(req)


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint — scraped every 10s by Prometheus."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    """Liveness probe for orchestrators."""
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/ready")
async def ready():
    """Readiness probe — checks Groq API key is configured."""
    try:
        get_groq_client()
        return {"status": "ready"}
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


# ── Load generator (for demo / learning) ────────────────────────
DEMO_PROMPTS = [
    "Explain the difference between precision and recall in machine learning.",
    "What is the transformer attention mechanism?",
    "How does RAG (Retrieval Augmented Generation) work?",
    "Explain distributed tracing in microservices.",
    "What are the RED metrics (Rate, Errors, Duration) in observability?",
    "Describe how Kafka consumer groups work.",
    "What is data drift and how do you detect it?",
    "Explain the difference between SLI, SLO, and SLA.",
    "How does a vector database work for semantic search?",
    "What is the difference between Loki and Elasticsearch for log storage?",
]


async def continuous_load_generator(rps: float = 0.5):
    """Generates realistic load for demo purposes. 0.5 rps = 1 request every 2s."""
    logger.info("load_generator_started", target_rps=rps)
    interval = 1.0 / rps

    while True:
        await asyncio.sleep(interval + random.uniform(-0.1, 0.1))
        prompt = random.choice(DEMO_PROMPTS)
        model = random.choice(["llama-3.1-8b-instant", "llama-3.1-8b-instant", "llama-3.3-70b-versatile"])

        req = InferenceRequest(
            prompt=prompt,
            model=model,
            max_tokens=random.randint(100, 400),
        )
        try:
            await run_llm_inference(req)
        except Exception as e:
            logger.warning("load_gen_request_failed", error=str(e))


@app.on_event("startup")
async def startup():
    logger.info("genai_pipeline_starting", service=SERVICE_NAME)
    # Start background load generator only in demo mode
    if os.getenv("DEMO_MODE", "true").lower() == "true":
        asyncio.create_task(continuous_load_generator(rps=0.3))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

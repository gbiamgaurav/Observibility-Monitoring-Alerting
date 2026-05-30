"""
Traditional ML Pipeline — Scikit-learn Inference Service

Simulates a production fraud-detection model with:
  - Feature extraction + validation
  - Model inference with confidence scoring
  - Data drift detection (KS-test)
  - Full Prometheus + OTel + structured log instrumentation

Run standalone:
  uvicorn pipeline:app --port 8002
"""

import os
import sys
import time
import json
import random
import asyncio
import numpy as np
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
import structlog
from kafka import KafkaProducer

from common.metrics import (
    ML_INFERENCE_REQUESTS, ML_INFERENCE_DURATION,
    ML_MODEL_ACCURACY, ML_DATA_DRIFT_SCORE,
    ML_FEATURE_MISSING_RATE, ML_PREDICTION_CONFIDENCE,
    ML_LAST_TRAINING_TS, KAFKA_MESSAGES_PROCESSED,
)
from common.logging_config import configure_logging, get_logger
from common.tracing import configure_tracing

# ── Bootstrap ─────────────────────────────────────────────────────
SERVICE_NAME = os.getenv("SERVICE_NAME", "ml-pipeline")
MODEL_NAME = "fraud-detector"
MODEL_VERSION = "v2.1.0"

configure_logging(SERVICE_NAME, os.getenv("LOG_LEVEL", "INFO"))
tracer = configure_tracing(SERVICE_NAME)
logger = get_logger(__name__)

app = FastAPI(title="ML Pipeline — Fraud Detection", version="1.0.0")

# ── Model Setup ────────────────────────────────────────────────────
# In production this loads from a model registry (MLflow, SageMaker, etc.)
# Here we train a simple demo model on synthetic data.

FEATURE_NAMES = [
    "transaction_amount", "hour_of_day", "day_of_week",
    "merchant_category", "user_age_days", "tx_count_24h",
    "amount_deviation", "is_international", "device_trust_score",
]

_model: Optional[RandomForestClassifier] = None
_scaler: Optional[StandardScaler] = None
_reference_distribution: Optional[np.ndarray] = None  # For drift detection


def train_demo_model():
    """Train a minimal fraud detection model on synthetic data."""
    global _model, _scaler, _reference_distribution

    np.random.seed(42)
    n_samples = 5000

    # Synthetic features
    X = np.column_stack([
        np.random.exponential(100, n_samples),          # transaction_amount
        np.random.randint(0, 24, n_samples),             # hour_of_day
        np.random.randint(0, 7, n_samples),              # day_of_week
        np.random.randint(0, 20, n_samples),             # merchant_category
        np.random.randint(30, 3650, n_samples),          # user_age_days
        np.random.poisson(3, n_samples),                 # tx_count_24h
        np.random.normal(0, 1, n_samples),               # amount_deviation
        np.random.binomial(1, 0.15, n_samples),          # is_international
        np.random.beta(8, 2, n_samples),                 # device_trust_score
    ])

    # Label: high amount + international + new user = more fraud
    fraud_score = (
        (X[:, 0] > 500).astype(float) * 0.4
        + X[:, 7] * 0.3
        + (X[:, 4] < 90).astype(float) * 0.2
        + (X[:, 5] > 10).astype(float) * 0.1
    )
    y = (fraud_score + np.random.normal(0, 0.1, n_samples) > 0.4).astype(int)

    _scaler = StandardScaler()
    X_scaled = _scaler.fit_transform(X)

    _model = RandomForestClassifier(n_estimators=50, random_state=42, n_jobs=-1)
    _model.fit(X_scaled, y)

    # Store reference distribution for drift detection
    _reference_distribution = X.copy()

    ML_LAST_TRAINING_TS.labels(
        service=SERVICE_NAME, model_name=MODEL_NAME
    ).set(time.time())

    # Estimate accuracy on training set (demo only — use holdout in prod)
    train_accuracy = _model.score(X_scaled, y)
    ML_MODEL_ACCURACY.labels(
        service=SERVICE_NAME, model_name=MODEL_NAME, model_version=MODEL_VERSION
    ).set(train_accuracy)

    logger.info(
        "model_trained",
        model=MODEL_NAME,
        version=MODEL_VERSION,
        accuracy=round(train_accuracy, 4),
        n_samples=n_samples,
    )


def ks_drift_score(reference: np.ndarray, current: np.ndarray) -> float:
    """Simple KS-test-based drift score. Returns 0 (no drift) to 1 (max drift)."""
    from scipy import stats
    stat, _ = stats.ks_2samp(reference, current)
    return float(stat)


# ── Request/Response models ────────────────────────────────────────
class FraudRequest(BaseModel):
    transaction_id: str
    transaction_amount: float
    hour_of_day: int
    day_of_week: int
    merchant_category: int
    user_age_days: int
    tx_count_24h: int
    amount_deviation: float
    is_international: bool
    device_trust_score: float


class FraudResponse(BaseModel):
    transaction_id: str
    is_fraud: bool
    fraud_probability: float
    confidence: float
    model_version: str
    latency_ms: float
    features_used: int


# ── Core inference function ────────────────────────────────────────
def predict_fraud(req: FraudRequest) -> FraudResponse:
    """Run fraud detection inference with full instrumentation."""

    start = time.time()

    with tracer.start_as_current_span("ml_inference") as span:
        span.set_attribute("model.name", MODEL_NAME)
        span.set_attribute("model.version", MODEL_VERSION)
        span.set_attribute("transaction.id", req.transaction_id)

        try:
            features = np.array([[
                req.transaction_amount,
                req.hour_of_day,
                req.day_of_week,
                req.merchant_category,
                req.user_age_days,
                req.tx_count_24h,
                req.amount_deviation,
                int(req.is_international),
                req.device_trust_score,
            ]])

            # Check for missing/invalid features
            missing = int(np.any(np.isnan(features)))
            if missing:
                ML_FEATURE_MISSING_RATE.labels(
                    service=SERVICE_NAME, model_name=MODEL_NAME, feature_name="any"
                ).set(1.0)

            features_scaled = _scaler.transform(features)
            proba = _model.predict_proba(features_scaled)[0]
            fraud_prob = float(proba[1])
            confidence = float(max(proba))
            is_fraud = fraud_prob > 0.5

            latency_ms = (time.time() - start) * 1000

            # ── Metrics ──────────────────────────────────────────
            ML_INFERENCE_REQUESTS.labels(
                service=SERVICE_NAME,
                model_name=MODEL_NAME,
                model_version=MODEL_VERSION,
                status="success",
            ).inc()
            ML_INFERENCE_DURATION.labels(
                service=SERVICE_NAME,
                model_name=MODEL_NAME,
                model_version=MODEL_VERSION,
            ).observe(latency_ms / 1000)
            ML_PREDICTION_CONFIDENCE.labels(
                service=SERVICE_NAME, model_name=MODEL_NAME, model_version=MODEL_VERSION
            ).observe(confidence)

            # ── Drift detection (sampled, not every call) ────────
            if random.random() < 0.01 and _reference_distribution is not None:
                feature_idx = random.randint(0, len(FEATURE_NAMES) - 1)
                drift = ks_drift_score(
                    _reference_distribution[:, feature_idx],
                    features[:, feature_idx],
                )
                ML_DATA_DRIFT_SCORE.labels(
                    service=SERVICE_NAME,
                    model_name=MODEL_NAME,
                    feature_name=FEATURE_NAMES[feature_idx],
                ).set(drift)

            span.set_attribute("prediction.fraud_prob", fraud_prob)
            span.set_attribute("prediction.confidence", confidence)
            span.set_attribute("prediction.is_fraud", is_fraud)
            span.set_status(Status(StatusCode.OK))

            logger.info(
                "ml_inference_success",
                transaction_id=req.transaction_id,
                is_fraud=is_fraud,
                fraud_probability=round(fraud_prob, 4),
                confidence=round(confidence, 4),
                latency_ms=round(latency_ms, 2),
                model_version=MODEL_VERSION,
            )

            return FraudResponse(
                transaction_id=req.transaction_id,
                is_fraud=is_fraud,
                fraud_probability=round(fraud_prob, 4),
                confidence=round(confidence, 4),
                model_version=MODEL_VERSION,
                latency_ms=round(latency_ms, 2),
                features_used=len(FEATURE_NAMES),
            )

        except Exception as e:
            latency_ms = (time.time() - start) * 1000
            ML_INFERENCE_REQUESTS.labels(
                service=SERVICE_NAME,
                model_name=MODEL_NAME,
                model_version=MODEL_VERSION,
                status="error",
            ).inc()
            span.set_status(Status(StatusCode.ERROR, str(e)))

            logger.error(
                "ml_inference_error",
                transaction_id=req.transaction_id,
                error=str(e),
                latency_ms=round(latency_ms, 2),
            )
            raise HTTPException(status_code=500, detail=f"Inference error: {e}")


# ── FastAPI Routes ──────────────────────────────────────────────────
@app.post("/predict", response_model=FraudResponse)
def predict(req: FraudRequest):
    return predict_fraud(req)


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME, "model": MODEL_NAME}


# ── Demo load generator ─────────────────────────────────────────────
async def ml_load_generator():
    """Send synthetic transactions for demo purposes."""
    await asyncio.sleep(5)
    while True:
        await asyncio.sleep(random.uniform(0.1, 0.5))  # 2–10 rps

        # Occasionally inject drift (high amounts)
        if random.random() < 0.1:
            amount = random.uniform(5000, 50000)  # Drifted distribution
        else:
            amount = random.expovariate(1 / 100)

        req = FraudRequest(
            transaction_id=f"tx-{int(time.time() * 1000)}-{random.randint(1000, 9999)}",
            transaction_amount=amount,
            hour_of_day=random.randint(0, 23),
            day_of_week=random.randint(0, 6),
            merchant_category=random.randint(0, 19),
            user_age_days=random.randint(1, 3650),
            tx_count_24h=random.poisson(3),
            amount_deviation=random.normalvariate(0, 1),
            is_international=random.random() < 0.15,
            device_trust_score=random.betavariate(8, 2),
        )
        try:
            predict_fraud(req)
        except Exception:
            pass


@app.on_event("startup")
async def startup():
    logger.info("ml_pipeline_starting", service=SERVICE_NAME)
    train_demo_model()
    if os.getenv("DEMO_MODE", "true").lower() == "true":
        asyncio.create_task(ml_load_generator())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)

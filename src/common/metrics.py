"""
Shared Prometheus metrics registry for all AI pipeline services.
Each service imports what it needs — avoids duplicate registration.
"""
from prometheus_client import (
    Counter, Histogram, Gauge, Summary,
    CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
)

# ── GenAI / LLM Metrics ─────────────────────────────────────────

GENAI_REQUESTS = Counter(
    "genai_requests_total",
    "Total LLM inference requests",
    ["service", "model", "status"],          # status: success | error | rate_limited
)

GENAI_REQUEST_DURATION = Histogram(
    "genai_request_duration_seconds",
    "End-to-end LLM request latency (including network)",
    ["service", "model"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
)

GENAI_TOKENS = Counter(
    "genai_tokens_total",
    "Total tokens processed",
    ["service", "model", "type"],            # type: input | output
)

GENAI_RATE_LIMIT_HITS = Counter(
    "genai_rate_limit_hits_total",
    "Number of Groq/LLM API rate limit responses",
    ["service", "model"],
)

GENAI_CONTEXT_UTILIZATION = Gauge(
    "genai_context_window_utilization",
    "Fraction of model context window used (0.0–1.0)",
    ["service", "model"],
)

GENAI_QUEUE_DEPTH = Gauge(
    "genai_queue_depth",
    "Number of pending LLM inference requests",
    ["service"],
)

# ── Agentic AI Metrics ──────────────────────────────────────────

AGENT_SESSIONS = Counter(
    "agent_sessions_total",
    "Total agentic sessions started",
    ["service", "agent_name", "status"],     # status: completed | failed | timeout | max_steps
)

AGENT_SESSION_DURATION = Histogram(
    "agent_session_duration_seconds",
    "Full agent session wall-clock duration",
    ["service", "agent_name"],
    buckets=[1, 5, 15, 30, 60, 120, 300, 600],
)

AGENT_STEPS = Counter(
    "agent_steps_total",
    "Total agent reasoning steps taken",
    ["service", "agent_name"],
)

AGENT_STEP_DURATION = Histogram(
    "agent_step_duration_seconds",
    "Duration of a single agent reasoning step",
    ["service", "agent_name"],
    buckets=[0.1, 0.5, 1.0, 3.0, 10.0, 30.0],
)

AGENT_TOOL_CALLS = Counter(
    "agent_tool_calls_total",
    "Total tool invocations by agents",
    ["service", "agent_name", "tool_name", "status"],
)

AGENT_MAX_STEPS_REACHED = Counter(
    "agent_max_steps_reached_total",
    "Sessions terminated because max_steps was hit (potential infinite loop)",
    ["service", "agent_name"],
)

AGENT_HALLUCINATIONS = Counter(
    "agent_hallucinations_detected_total",
    "Detected hallucinations in agent output (via guardrails)",
    ["service", "agent_name"],
)

AGENT_QUEUE_DEPTH = Gauge(
    "agent_queue_depth",
    "Pending agent tasks in queue",
    ["service"],
)

# ── Traditional ML Metrics ──────────────────────────────────────

ML_INFERENCE_REQUESTS = Counter(
    "ml_inference_requests_total",
    "Total ML model inference calls",
    ["service", "model_name", "model_version", "status"],
)

ML_INFERENCE_DURATION = Histogram(
    "ml_inference_duration_seconds",
    "Model inference latency",
    ["service", "model_name", "model_version"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0],
)

ML_MODEL_ACCURACY = Gauge(
    "ml_model_accuracy",
    "Current rolling model accuracy (last N predictions)",
    ["service", "model_name", "model_version"],
)

ML_DATA_DRIFT_SCORE = Gauge(
    "ml_data_drift_score",
    "KS-test data drift score per feature (0=no drift, 1=max drift)",
    ["service", "model_name", "feature_name"],
)

ML_FEATURE_MISSING_RATE = Gauge(
    "ml_feature_missing_rate",
    "Fraction of requests with missing values for this feature",
    ["service", "model_name", "feature_name"],
)

ML_PREDICTION_CONFIDENCE = Histogram(
    "ml_prediction_confidence",
    "Distribution of prediction confidence scores",
    ["service", "model_name", "model_version"],
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)

ML_LAST_TRAINING_TS = Gauge(
    "ml_last_training_completed_timestamp",
    "Unix timestamp of last successful model training run",
    ["service", "model_name"],
)

# ── Kafka Consumer Metrics ──────────────────────────────────────

KAFKA_MESSAGES_PROCESSED = Counter(
    "kafka_messages_processed_total",
    "Messages consumed and processed from Kafka",
    ["service", "topic", "status"],
)

KAFKA_PROCESSING_DURATION = Histogram(
    "kafka_message_processing_duration_seconds",
    "Time to process a single Kafka message",
    ["service", "topic"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0],
)

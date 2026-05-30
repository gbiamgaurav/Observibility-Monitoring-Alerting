# Production Observability, Monitoring & Alerting
### For ML Pipelines, GenAI Services, and Agentic AI Systems

> **Learning Goal**: Understand how to detect, diagnose, and resolve production outages in AI systems using real tools used at companies like Uber, Airbnb, Netflix, and OpenAI.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [How to Read Each Tool](#2-how-to-read-each-tool)
3. [The Three Pillars of Observability](#3-the-three-pillars)
4. [Quick Start](#4-quick-start)
5. [Pipeline Descriptions](#5-pipeline-descriptions)
6. [Key Metrics Reference](#6-key-metrics-reference)
7. [Production Outage Checklist](#7-production-outage-checklist)
8. [Grafana Dashboards Guide](#8-grafana-dashboards-guide)
9. [Kibana Log Queries](#9-kibana-log-queries)
10. [Prometheus Query Reference](#10-prometheus-query-reference)
11. [Alert Runbooks](#11-alert-runbooks)
12. [Kafka Monitoring](#12-kafka-monitoring)
13. [BigQuery & Multi-Cloud Log Analytics](#13-bigquery--multi-cloud-log-analytics)
14. [Distributed Tracing with Jaeger](#14-distributed-tracing-with-jaeger)
15. [Hands-On Labs — Simulate Outages](#15-hands-on-labs)
16. [SLI, SLO, SLA Definitions](#16-sli-slo-sla-definitions)
17. [Production Best Practices](#17-production-best-practices)

---

## 1. Architecture Overview

```
                    ┌─────────────────────────────────────────────┐
                    │              AI PIPELINE SERVICES            │
                    │                                              │
                    │  ┌─────────────────┐  ┌──────────────────┐  │
                    │  │  GenAI Pipeline  │  │ Agentic Pipeline │  │
                    │  │  (Groq LLM API) │  │  (ReAct Agent)   │  │
                    │  │  :8001          │  │  :8003           │  │
                    │  └────────┬────────┘  └───────┬──────────┘  │
                    │           │                    │             │
                    │  ┌────────┴────────────────────┴──────────┐  │
                    │  │          ML Pipeline (Sklearn)          │  │
                    │  │          Fraud Detector  :8002          │  │
                    │  └────────────────────┬────────────────────┘  │
                    └───────────────────────┼─────────────────────┘
                                            │ Events, Logs, Traces
                    ┌───────────────────────▼─────────────────────┐
                    │              KAFKA EVENT BUS                  │
                    │  Topics: genai-events, agent-events,         │
                    │          ml-events, pipeline-logs            │
                    │  :9092 (external) :29092 (internal)          │
                    └───────────────────────┬──────────────────────┘
                                            │
          ┌─────────────────────────────────▼───────────────────────────┐
          │                    OBSERVABILITY STACK                       │
          │                                                              │
          │  ┌──────────────┐  ┌──────────────┐  ┌───────────────────┐  │
          │  │  PROMETHEUS  │  │    LOKI      │  │  ELASTICSEARCH    │  │
          │  │  :9090       │  │    :3100     │  │  :9200            │  │
          │  │  Metrics     │  │    Logs      │  │  Structured Logs  │  │
          │  └──────┬───────┘  └──────┬───────┘  └─────────┬─────────┘  │
          │         │                 │                     │            │
          │  ┌──────▼─────────────────▼─────────────────────▼─────────┐  │
          │  │                    GRAFANA :3000                        │  │
          │  │         Unified Dashboards (Metrics + Logs + Traces)    │  │
          │  └─────────────────────────────────────────────────────────┘  │
          │                                                              │
          │  ┌──────────────┐  ┌──────────────┐  ┌───────────────────┐  │
          │  │  ALERTMANAGER│  │   KIBANA     │  │  JAEGER           │  │
          │  │  :9093       │  │   :5601      │  │  :16686           │  │
          │  │ Routes alerts│  │  Log Search  │  │ Distributed Trace │  │
          │  └──────────────┘  └──────────────┘  └───────────────────┘  │
          │                                                              │
          │  ┌──────────────┐  ┌──────────────────────────────────────┐  │
          │  │OTEL COLLECTOR│  │  LOGSTASH :5044/:5000                │  │
          │  │ :4317/:4318  │  │  Log processing & enrichment         │  │
          │  │ Trace gateway│  └──────────────────────────────────────┘  │
          │  └──────────────┘                                            │
          └──────────────────────────────────────────────────────────────┘
                    │
          ┌─────────▼──────────────────────────┐
          │   GOOGLE BIGQUERY (default)        │
          │   Historical log analysis          │
          │   GCS-backed external SQL tables   │
          │   Also supported: Athena · Synapse │
          └────────────────────────────────────┘
```

---

## 2. How to Read Each Tool

> **Start here if this is your first time.** Each tool answers one specific question. You don't need to use all of them at once — follow the flow below.

---

### The Decision Flow

```
Something feels wrong
        │
        ▼
① Prometheus :9090/alerts     "Which service is broken?"
        │
        ▼
② Grafana :3000               "How bad is it, and since when?"
        │
        ▼
③ Jaeger :16686               "Which exact request failed?"
        │
        ▼
④ Kibana :5601                "What did the error message say?"
```

Start at the top. Stop as soon as you have enough to act.

---

### ① Prometheus — http://localhost:9090

**One sentence**: Prometheus stores numbers over time (metrics) and fires alerts when they cross thresholds.

**What you'll see on the home page**:
- `/alerts` — traffic-light view. **Red = something is broken right now.** Green = all clear.
- `/targets` — list of services Prometheus is scraping. All should say `UP`.
- The query box — paste PromQL queries to graph any metric.

**How to use it**:

| You want to know | Type this in the query box |
|---|---|
| Are all services up? | `up` |
| GenAI error rate | `rate(genai_requests_total{status="error"}[5m])` |
| Kafka consumer lag | `kafka_consumergroup_lag` |
| Is CPU high? | `rate(node_cpu_seconds_total{mode="idle"}[5m])` |

**How to interpret results**:
- A flat line near zero = healthy
- A spike = something happened at that moment
- A line climbing and not coming back down = a growing problem (e.g. Kafka lag)

**Alerts page** (`/alerts`) — the most important page in this stack:
- `Inactive` = never fired (good)
- `Pending` = threshold crossed but not yet long enough to page (warning)
- `Firing` = alert is active right now (act immediately)

---

### ② Grafana — http://localhost:3000 (admin / admin123)

**One sentence**: Grafana turns Prometheus numbers into visual dashboards so you can see trends, spikes, and patterns at a glance.

**What you'll see**:
- A grid of panels — each panel is one metric graphed over time
- Time range picker (top right) — default is "Last 6 hours". Change to "Last 15 min" during an incident
- Each panel has a colored threshold line: green → yellow → red

**How to read a panel**:

```
Panel: GenAI Error Rate
  │
  │  5% ─────────────────── RED threshold (alert fires here)
  │
  │  1% ─────────────────── YELLOW threshold (warning)
  │
  │  0% ──────▁▁▁▁▁▁▁▂▂▁▁── normal (good, line near zero)
  │
  └──────────────────────────▶ time
```

**What each color means**:
- Green panel = metric is within normal range
- Yellow panel = approaching the alert threshold, watch it
- Red panel = threshold breached, an alert has likely fired

**To add the standard infrastructure dashboard** (first time setup):
```
Dashboards → New → Import → Enter ID: 1860 → Load → Select Prometheus datasource → Import
```
This gives you CPU, memory, disk, and network for all containers.

**Explore tab** (left sidebar): Use this to write live log queries against Loki without a pre-built dashboard. Good for searching logs during an incident.

---

### ③ Jaeger — http://localhost:16686

**One sentence**: Jaeger records the full journey of every request through your services, so you can see exactly which step was slow or failed.

**What you'll see on the search page**:
- A dropdown of services (genai-pipeline, ml-pipeline, agentic-pipeline)
- A list of recent traces, each showing total duration and number of spans
- Traces with errors are highlighted in red/orange

**Step-by-step: finding a slow or broken request**:

```
1. Pick a Service       → e.g. "genai-pipeline"
2. Set Min Duration     → "5s"  (filters to slow requests only)
3. Click Find Traces
4. Click the slowest trace
5. You'll see a waterfall chart — each bar is one step
6. The widest bar = the bottleneck
7. A red bar = an error occurred at that step
```

**What a healthy trace looks like**:
```
genai_request ──────────────────── 1.2s total
  └── llm_inference ───────────── 1.1s   ← most time in LLM call (normal)
```

**What a broken trace looks like**:
```
agent_session ─────────────────────────────── 48s total
  ├── agent_step_1 ──── 2s
  ├── agent_step_2 ──── 2s
  ├── agent_step_3 ──── 2s   ← same steps repeating (agent loop!)
  ├── agent_step_4 ──── 2s
  └── agent_step_5 ──── 2s  [ERROR: max_steps_reached]
```

**Key tip**: Click any span to see its attributes — model name, token count, error message, request ID. Copy the `request_id` and use it in Kibana to find the matching logs.

---

### ④ Kibana — http://localhost:5601

**One sentence**: Kibana lets you search through the full text of every log line your services wrote, so you can read the actual error messages.

**First time setup** (one-time, takes 30 seconds):
```
1. Go to http://localhost:5601
2. Click "Stack Management" (bottom left gear icon)
3. Click "Index Patterns" → "Create index pattern"
4. Type: pipeline-logs-*
5. Time field: @timestamp
6. Save → Click "Discover" in the left sidebar
```

**How to search logs**:

The search bar at the top accepts KQL (Kibana Query Language). Think of it like a Google search for your logs.

| You want to find | Type this |
|---|---|
| All errors | `level: ERROR` |
| GenAI errors only | `service: "genai-pipeline" AND level: ERROR` |
| Slow requests (>5s) | `latency_ms > 5000` |
| One specific request | `request_id: "req-abc-123"` |
| Rate limit events | `event: "llm_rate_limited"` |
| Agent loop events | `event: "agent_max_steps_reached"` |

**How to read a log entry** (click any row to expand it):
```json
{
  "timestamp": "2026-05-30T10:23:45Z",   ← when it happened
  "level": "ERROR",                       ← severity
  "service": "genai-pipeline",            ← which service
  "event": "llm_api_error",              ← what happened
  "request_id": "req-1234",              ← use this in Jaeger
  "error": "Rate limit exceeded",        ← the actual error message
  "latency_ms": 234                      ← how long the request took
}
```

**Time filter**: Always narrow the time range first (top right) — searching "last 15 minutes" is much faster than "last 7 days".

---

### ⑤ Kafka UI — http://localhost:8080

**One sentence**: Kafka UI shows you whether messages are flowing through the event bus and whether consumers are keeping up.

**The one number that matters**: **Consumer Lag**

```
Topics → pipeline-events → Consumer Groups
  ml-pipeline-consumer    lag: 0       ← healthy (keeping up)
  ml-pipeline-consumer    lag: 50,000  ← critical (badly behind)
```

- Lag = 0 → consumer is reading messages as fast as they arrive
- Lag growing over time → consumer is overwhelmed or crashed
- Lag draining → consumer is catching up after a recovery

**What to check during an incident**:
1. **Brokers** tab — is Kafka itself healthy? Should show 1 broker, all green
2. **Topics** tab — which topics have messages? Is the message rate normal?
3. **Consumer Groups** tab — is lag zero or growing?

---

### ⑥ Alertmanager — http://localhost:9093

**One sentence**: Alertmanager receives alerts from Prometheus and shows you what is currently firing, silenced, or inhibited.

**What you'll see**:
- A list of currently firing alerts with their labels and start time
- Each alert shows: name, severity, which service, how long it's been firing

**How to silence an alert** (e.g. during planned maintenance):
```
Click the alert → "Silence" → Set duration → Add comment → Create
```
This stops notifications for that alert without resolving it — useful when you know about the issue and are working on it.

---

### Quick Reference: Which tool for which question?

| Question | Tool | URL |
|---|---|---|
| Is anything broken right now? | Prometheus Alerts | :9090/alerts |
| How long has this been happening? | Grafana | :3000 |
| Which specific request failed? | Jaeger | :16686 |
| What was the error message? | Kibana | :5601 |
| Is Kafka falling behind? | Kafka UI | :8080 |
| What alerts are currently firing? | Alertmanager | :9093 |

---

## 3. The Three Pillars

Observability rests on three complementary signal types. You need all three to diagnose a production outage.

### Pillar 1: Metrics (Prometheus + Grafana)

**What**: Numeric time-series data. Counts, rates, histograms, gauges.

**Why**: Tells you WHAT is broken and HOW BAD it is. Metrics are cheap to store and fast to query.

**Examples for AI pipelines**:

| Metric | Type | What it tells you |
|--------|------|-------------------|
| `genai_request_duration_seconds` | Histogram | LLM latency distribution |
| `genai_tokens_total{type="output"}` | Counter | Token usage (= cost) |
| `agent_max_steps_reached_total` | Counter | Agent infinite loops |
| `ml_data_drift_score` | Gauge | Model staleness signal |
| `kafka_consumergroup_lag` | Gauge | Pipeline falling behind |

**RED Method** (for services):
- **R**ate: How many requests per second?
- **E**rrors: What fraction are failing?
- **D**uration: How long are they taking?

**USE Method** (for resources):
- **U**tilization: How busy is the resource? (CPU 80%)
- **S**aturation: Is work queuing up? (Kafka lag)
- **E**rrors: Are there hardware/system errors?

---

### Pillar 2: Logs (Loki + Kibana + Elasticsearch)

**What**: Timestamped text records of events. Structured JSON logs are mandatory in production.

**Why**: Tells you WHY it's broken. Logs contain context metrics can't capture (request IDs, user data, stack traces).

**Structured log format** (what our pipelines emit):
```json
{
  "timestamp": "2025-01-15T10:23:45.123Z",
  "level": "ERROR",
  "service": "genai-pipeline",
  "event": "llm_api_error",
  "request_id": "req-1705312425123",
  "model": "llama3-70b-8192",
  "latency_ms": 234.5,
  "error_type": "APIStatusError",
  "error": "Rate limit exceeded",
  "environment": "production"
}
```

**Two log backends in this stack**:
- **Loki** (with Grafana): Lightweight, label-based querying. Best for tail-and-search during live incidents.
- **Elasticsearch + Kibana**: Full-text search, aggregations, dashboards. Best for deep analysis and historical queries.

---

### Pillar 3: Traces (OpenTelemetry + Jaeger)

**What**: End-to-end request journey across services. Shows parent-child relationships between function calls.

**Why**: Tells you WHERE in the call chain it's breaking. Critical for distributed systems and multi-step agent workflows.

**What a trace looks like for an Agentic AI call**:
```
agent_session (total: 45.2s)
├── agent_step_1 (2.1s)
│   └── llm_inference (1.8s)       ← LLM call
├── agent_step_2 (3.4s)
│   ├── llm_inference (1.2s)
│   └── tool:web_search (2.1s)     ← External tool call
├── agent_step_3 (12.8s)
│   ├── llm_inference (1.5s)
│   └── tool:calculator (0.001s)
└── agent_step_4 (0.9s)            ← Final answer
    └── llm_inference (0.8s)
```

---

## 4. Quick Start

### Prerequisites

- Docker Desktop 4.0+ with at least **8GB RAM** allocated
- `docker compose` v2+
- A free Groq API key from [console.groq.com](https://console.groq.com) (takes 30 seconds)
- Python 3.13+ with [uv](https://github.com/astral-sh/uv) (`brew install uv`)
- A Google Cloud account with the `observibility-monitoring` project

### Step 1: Clone and Configure

```bash
git clone https://github.com/YOUR_USERNAME/Observibility-Monitoring-Alerting.git
cd Observibility-Monitoring-Alerting

# Copy environment template and fill in your keys
cp .env.example .env
nano .env   # set GROQ_API_KEY at minimum
```

### Step 2: Python Environment

```bash
# Create virtualenv and install all dependencies
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

### Step 3: Google Cloud Authentication

Service account key creation is disabled on most GCP organisations. Use Application Default Credentials (ADC) instead — works automatically with all Google Cloud client libraries.

```bash
# Install gcloud CLI (if not already installed)
brew install --cask google-cloud-sdk

# Authenticate — opens browser, saves creds to ~/.config/gcloud/
gcloud auth application-default login

# Verify authentication
gcloud auth application-default print-access-token
```

Then confirm your `.env` contains:
```
GOOGLE_APPLICATION_CREDENTIALS=/Users/<you>/.config/gcloud/application_default_credentials.json
GCP_PROJECT_ID=observibility-monitoring
GCS_LOG_BUCKET=gs://observibility-monitoring/pipeline-logs/
```

### Step 4: BigQuery Setup (one-time)

```bash
# Create the dataset
bq mk --dataset --location=US observibility-monitoring:pipeline_observability

# Register the external tables over GCS
bq query --project_id=observibility-monitoring --use_legacy_sql=false < bigquery/setup.sql
```

### Step 5: Start the Full Stack

```bash
# Start all services (takes 2-3 minutes on first run)
docker compose up -d

# Watch startup logs
docker compose logs -f

# Verify all services are healthy
docker compose ps
```

### Step 6: Verify Services Are Healthy

```bash
# Check that pipelines are responding
curl http://localhost:8001/health   # GenAI
curl http://localhost:8002/health   # ML
curl http://localhost:8003/health   # Agentic

# Verify metrics are being emitted
curl http://localhost:8001/metrics | grep -E "genai_requests|genai_tokens"

# Verify Prometheus is scraping (should show all targets "UP")
open http://localhost:9090/targets
```

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin123 |
| Prometheus | http://localhost:9090 | — |
| AlertManager | http://localhost:9093 | — |
| Kibana | http://localhost:5601 | — |
| Jaeger | http://localhost:16686 | — |
| Kafka UI | http://localhost:8080 | — |
| GenAI API | http://localhost:8001/docs | — |
| ML API | http://localhost:8002/docs | — |
| Agent API | http://localhost:8003/docs | — |

### Step 7: Run Your First Requests

```bash
# Test GenAI pipeline
curl -X POST http://localhost:8001/infer \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "What is observability in distributed systems?",
    "model": "llama-3.1-8b-instant",
    "max_tokens": 200
  }'

# Test ML pipeline (fraud detection)
curl -X POST http://localhost:8002/predict \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "tx-001",
    "transaction_amount": 5000.0,
    "hour_of_day": 2,
    "day_of_week": 6,
    "merchant_category": 15,
    "user_age_days": 10,
    "tx_count_24h": 25,
    "amount_deviation": 8.5,
    "is_international": true,
    "device_trust_score": 0.1
  }'

# Test Agentic pipeline
curl -X POST http://localhost:8003/run \
  -H "Content-Type: application/json" \
  -d '{
    "task": "Research LLM observability best practices and summarize key metrics.",
    "agent_name": "research-agent",
    "max_steps": 5
  }'
```

---

## 5. Pipeline Descriptions

### GenAI Pipeline (Port 8001)

**What it does**: HTTP API that calls Groq's LLM API to answer questions. Groq runs open models (Llama 3, Mixtral) at extremely fast speeds via custom hardware.

**Tech stack**: FastAPI + Groq Python SDK + Prometheus client + OpenTelemetry

**Key instrumentation points**:
```python
# Every LLM call records 5 metrics automatically:
GENAI_REQUESTS.labels(service, model, status).inc()          # total count
GENAI_REQUEST_DURATION.observe(latency_seconds)              # latency histogram
GENAI_TOKENS.labels(..., type="input").inc(prompt_tokens)    # input token counter
GENAI_TOKENS.labels(..., type="output").inc(completion_tokens)
GENAI_RATE_LIMIT_HITS.inc()                                  # when Groq returns 429
```

**What can go wrong**:
1. Groq API rate limits → 429 responses, requests fail
2. Groq API outage → 502 responses
3. Very long prompts saturating context window → silent truncation, wrong answers
4. Token cost runaway → prompt injection or loops generating huge outputs
5. Large model latency → llama3-70b is 5-10x slower than llama3-8b

**Models available on Groq (free tier)**:
```
llama-3.1-8b-instant    ← Default (fast, 128k context, good for most tasks)
llama-3.3-70b-versatile ← More capable, used for agent reasoning
gemma2-9b-it            ← Google's Gemma 2 model
```

---

### Agentic AI Pipeline (Port 8003)

**What it does**: Implements a **ReAct** (Reasoning + Acting) agent that breaks complex tasks into steps, calling tools as needed.

**The ReAct Loop**:
```
Step 1: LLM REASONS about the task
Step 2: LLM DECIDES which tool to call (or gives final answer)
Step 3: Tool EXECUTES and returns result
Step 4: LLM OBSERVES the result
Step 5: Go back to Step 1 (or end if answer found)
```

**Tools the agent has**:
- `web_search(query)` — simulated web search results
- `calculator(expression)` — safe arithmetic evaluation
- `get_current_metrics(service)` — queries live service metrics

**Key instrumentation points**:
```python
# Per session (end-to-end agent lifecycle)
AGENT_SESSIONS.labels(service, agent_name, status).inc()
AGENT_SESSION_DURATION.observe(total_seconds)

# Per reasoning step (each LLM call in the loop)
AGENT_STEPS.inc()
AGENT_STEP_DURATION.observe(step_seconds)

# Per tool invocation
AGENT_TOOL_CALLS.labels(..., tool_name, status).inc()

# Safety / anomaly detection
AGENT_MAX_STEPS_REACHED.inc()     # potential infinite loop
AGENT_HALLUCINATIONS.inc()        # from guardrail checks
```

**What can go wrong**:
1. Agent reasoning in circles → hits `max_steps` limit
2. Tool returning empty/error results → agent retries indefinitely
3. Long session history exhausting LLM context window (32k tokens)
4. Cost runaway: 10 steps × 3 LLM calls/step = 30 Groq API calls per task
5. Hallucinated tool arguments causing cascading errors

---

### ML Pipeline — Fraud Detector (Port 8002)

**What it does**: Trains a Random Forest classifier on synthetic fraud data at startup, then serves real-time predictions.

**Feature engineering**:
```python
features = [
    transaction_amount,    # Amount being charged
    hour_of_day,          # Time of day (fraud peaks at 2-4am)
    day_of_week,          # Weekends have different patterns
    merchant_category,    # Type of merchant (0-19)
    user_age_days,        # How old is this account?
    tx_count_24h,         # Number of transactions in last 24h
    amount_deviation,     # Z-score: how unusual is this amount?
    is_international,     # Cross-border transaction?
    device_trust_score,   # Device fingerprint trust (0-1)
]
```

**Data drift detection** — built in using KS-test:
```python
# Every ~1% of requests, compare current vs reference distribution
ks_stat, p_value = scipy.stats.ks_2samp(reference_dist, current_dist)
ML_DATA_DRIFT_SCORE.set(ks_stat)   # 0 = no drift, 1 = max drift
# Alert threshold: ks_stat > 0.3
```

**What can go wrong**:
1. Input feature distribution shifts (e.g., average transaction amount 10x larger)
2. Model accuracy degrades without retraining
3. High missing feature rate from upstream data pipeline failure
4. Low prediction confidence (model is genuinely uncertain)
5. Training pipeline stalls — model serving stale predictions

---

## 6. Key Metrics Reference

### GenAI / LLM Metrics

```promql
# Request Rate (requests/second)
rate(genai_requests_total[5m])

# Error Rate (%) — if > 5%, alert is firing
rate(genai_requests_total{status="error"}[5m])
/ rate(genai_requests_total[5m]) * 100

# P99 Latency — SLO threshold is 10s
histogram_quantile(0.99, rate(genai_request_duration_seconds_bucket[5m]))

# Token throughput (output tokens/minute = cost driver)
rate(genai_tokens_total{type="output"}[1m]) * 60

# Rate limit hit frequency
rate(genai_rate_limit_hits_total[5m])

# Context window saturation (> 0.85 = truncation risk)
genai_context_window_utilization
```

### Agentic AI Metrics

```promql
# Session success rate
rate(agent_sessions_total{status="completed"}[10m])
/ rate(agent_sessions_total[10m]) * 100

# Loop detection: % of sessions hitting max_steps
rate(agent_max_steps_reached_total[10m])
/ rate(agent_sessions_total[10m]) * 100

# Tool success rate by tool name
rate(agent_tool_calls_total{status="success"}[5m])
/ rate(agent_tool_calls_total[5m])

# Average steps per session (healthy: 3-6)
increase(agent_steps_total[10m])
/ increase(agent_sessions_total{status="completed"}[10m])

# P95 session duration (SLO: < 120s)
histogram_quantile(0.95, rate(agent_session_duration_seconds_bucket[10m]))
```

### ML Pipeline Metrics

```promql
# Inference throughput
rate(ml_inference_requests_total{status="success"}[5m])

# P99 inference latency (SLO: < 2s)
histogram_quantile(0.99, rate(ml_inference_duration_seconds_bucket[5m]))

# Data drift scores — all features at once
ml_data_drift_score

# Median prediction confidence (healthy: > 0.7)
histogram_quantile(0.50, rate(ml_prediction_confidence_bucket[10m]))

# Model accuracy (should be > 0.80)
ml_model_accuracy
```

### Kafka Metrics

```promql
# Consumer lag per group (critical if > 10,000)
kafka_consumergroup_lag

# Is lag growing or shrinking?
# Positive = falling behind (bad), Negative = catching up (good)
deriv(kafka_consumergroup_lag[5m])

# Message production rate
rate(kafka_topic_partition_current_offset[1m])
```

---

## 7. Production Outage Checklist

When an alert fires, follow this systematic investigation process.

### Step 1: Triage (< 2 minutes)

```
□ What alert fired? (check Slack, PagerDuty, or AlertManager UI)
□ What severity? (Critical = wake someone up now, Warning = business hours)
□ Which service? (genai, ml, agentic, kafka, infra)
□ When did it start? (Prometheus timeline)
□ Is it still firing?
  → http://localhost:9090/alerts   (all active alerts)
  → http://localhost:9093          (AlertManager routing view)
```

---

### Step 2: Metrics Snapshot (2-5 minutes)

Open **Grafana** → relevant service dashboard. Check in order:

```
□ REQUEST RATE: Is traffic normal, spiked, or ZERO?
  If ZERO for 5+ min → service is dead, check docker-compose ps

□ ERROR RATE: What % of requests are failing?
  > 1%  → investigate immediately
  > 5%  → critical, likely user-visible outage

□ LATENCY P99: How slow are the worst requests?
  GenAI SLO:  < 10s
  ML SLO:     < 2s

□ UPSTREAM DEPENDENCIES:
  Kafka consumer lag growing?    → kafka_consumergroup_lag
  Groq API rate limits spiking?  → genai_rate_limit_hits_total
  CPU / Memory saturated?        → node-exporter metrics
```

**Quick Prometheus queries**:
```promql
# Are all pipeline services up?
up{job=~"genai-pipeline|ml-pipeline|agentic-pipeline"}

# Error rates across all services
{__name__=~".*_requests_total|.*_inference_requests_total", status="error"}
```

---

### Step 3: Log Investigation (5-15 minutes)

#### Kibana (http://localhost:5601) — full-text search

```
# All errors in last hour
level:(ERROR OR CRITICAL) AND @timestamp > now-1h

# GenAI errors only
service:"genai-pipeline" AND level:ERROR

# Rate limit events specifically
event:"llm_rate_limited" AND @timestamp:[now-30m TO now]

# High latency LLM calls (> 5 seconds)
service:"genai-pipeline" AND latency_ms:>5000

# Agent sessions that hit max steps
event:"agent_max_steps_reached"

# All errors across all services
level:(ERROR OR CRITICAL) AND @timestamp:[now-1h TO now]

# Find a specific request (use request_id from alert/user report)
request_id:"req-1705312425123"
```

#### Grafana Loki (Explore tab) — LogQL

```logql
# All errors from GenAI service
{service="genai-pipeline"} | json | level="ERROR"

# Rate limit events
{service="genai-pipeline"} | json | event="llm_rate_limited"

# Agent max steps events
{service="agentic-pipeline"} | json | event="agent_max_steps_reached"

# High latency calls (extract field and filter)
{service="genai-pipeline"} | json | latency_ms > 5000

# Error rate per minute (for time-series in logs)
rate({service="genai-pipeline"} | json | level="ERROR" [1m])
```

#### Docker logs (when services crash)

```bash
# Last 100 lines from a service
docker compose logs --tail=100 genai-pipeline

# Follow logs in real-time
docker compose logs -f genai-pipeline

# Get logs from a specific time window
docker compose logs --since="2025-01-15T10:00:00" genai-pipeline
```

---

### Step 4: Distributed Trace Lookup (for multi-hop issues)

1. Go to **Jaeger UI**: http://localhost:16686
2. Select service: `genai-pipeline` or `agentic-pipeline`
3. Set time range to the incident window
4. Filter options:
   - **Min Duration**: Set to `5s` to find slow requests
   - **Tags**: `error=true` to find failed requests
5. Click on a trace to see the full span tree

**For an agent that took too long**:
- Root span: `agent_session` — note total duration
- Count child spans: each `agent_step_N` = one reasoning loop
- Find which step took the longest
- Check if any tool call span shows a timeout

---

### Step 5: Root Cause Matrix

| Alert | Most Likely Cause | Confirming Query |
|-------|------------------|-----------------|
| `GenAIHighErrorRate` | Groq API rate limited | `rate(genai_rate_limit_hits_total[5m])` |
| `GenAIHighLatency` | Large model or max_tokens too high | Logs: `latency_ms > 5000`, check `model` field |
| `AgentMaxStepsReached` | Circular tool call pattern | Jaeger: spans with step count = max_steps |
| `MLDataDriftDetected` | Feature distribution shift | `ml_data_drift_score > 0.3` by feature |
| `KafkaConsumerLagHigh` | Consumer crashed or slow | `up{job="ml-pipeline"}`, check `docker ps` |
| `ServiceDown` | Container crashed / OOM | `docker compose ps`, `docker compose logs` |
| `MLModelAccuracyDrop` | Stale model + drifted data | `ml_model_accuracy`, trigger retraining |

---

### Step 6: Mitigation Commands

```bash
# Restart a crashed service
docker compose restart genai-pipeline

# Check why a service crashed
docker compose logs --tail=200 genai-pipeline | grep -E "ERROR|CRITICAL|panic|killed"

# Scale up a service (add more instances)
docker compose up -d --scale ml-pipeline=3

# Check Kafka consumer group lag
docker exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --all-groups

# Reset Kafka consumer to latest (skip stale messages — use carefully)
docker exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group ml-pipeline-consumer \
  --topic pipeline-events \
  --reset-offsets --to-latest --execute

# Force Prometheus to reload rules (no restart needed)
curl -X POST http://localhost:9090/-/reload

# Check active AlertManager alerts via API
curl http://localhost:9093/api/v2/alerts | python3 -m json.tool

# Silence an alert during maintenance (via AlertManager API)
curl -X POST http://localhost:9093/api/v2/silences \
  -H "Content-Type: application/json" \
  -d '{
    "matchers": [{"name": "alertname", "value": "GenAIHighLatency", "isRegex": false}],
    "startsAt": "2025-01-15T10:00:00Z",
    "endsAt": "2025-01-15T12:00:00Z",
    "createdBy": "engineer-on-call",
    "comment": "Scheduled maintenance window"
  }'
```

---

## 8. Grafana Dashboards Guide

Login: http://localhost:3000 (admin / admin123)

### How to Read the GenAI Dashboard

**Panel: Request Rate** (top-left)
- Normal: Steady line matching your expected load
- Alert: Sudden drop to 0 = service dead. Sudden spike = load surge

**Panel: Error Rate** (top-right)
- Normal: Near 0%
- Warning: > 1%. Critical: > 5%
- Color coding: green (ok) → yellow (warning) → red (critical)

**Panel: Latency Heatmap**
- X-axis: time. Y-axis: latency buckets. Color: request volume
- Normal: Most cells in the green low-latency buckets
- Alert: Red/yellow cells appearing in high-latency rows

**Panel: Token Usage Counter**
- Should grow linearly with traffic
- Sudden exponential growth = cost alert, possible prompt injection

**Panel: Rate Limit Events**
- Should always be 0. Any non-zero value = contact Groq or implement backoff

**Panel: Context Window Utilization**
- Keep below 80%. Above that = silent truncation risk

### How to Read the Agentic Dashboard

**Panel: Session Success Rate** — target > 95%

**Panel: Average Steps Per Session**
- Healthy range: 3-6 steps
- If consistently > 8: review agent prompts for unnecessary loops

**Panel: Max Steps Reached Rate**
- Should be near 0
- Any sustained value = agents stuck in loops — investigate task patterns

**Panel: Tool Success Rate by Tool**
- Per-tool breakdown in a table or bar chart
- Identify which tools are unreliable and fix them

---

## 9. Kibana Log Queries

### Setting Up Kibana (first time)

```
1. Navigate to http://localhost:5601
2. Go to: Stack Management → Index Patterns → Create index pattern
3. Pattern: pipeline-logs-*
4. Time field: @timestamp
5. Save → Navigate to Discover
```

### Essential KQL Query Examples

```kql
# All errors in last hour
level:(ERROR OR CRITICAL) AND @timestamp > now-1h

# GenAI pipeline errors only
service:"genai-pipeline" AND level:ERROR

# Rate limit events (Groq throttling)
event:"llm_rate_limited"

# High latency LLM calls (over 5 seconds)
latency_ms > 5000 AND service:"genai-pipeline"

# Agent sessions hitting max steps (potential loops)
event:"agent_max_steps_reached"

# Agent sessions that didn't complete normally
event:"agent_session_completed" AND status:"max_steps_reached"

# ML fraud model — low confidence predictions
service:"ml-pipeline" AND confidence < 0.6

# Anomalies flagged by Logstash enrichment
anomaly:*

# All errors across all services in incident window
level:(ERROR OR CRITICAL) AND @timestamp:[2025-01-15T10:00:00 TO 2025-01-15T12:00:00]

# Find all events for a specific request ID (correlate with traces)
request_id:"req-1705312425123"

# Slow agent sessions (over 60 seconds)
service:"agentic-pipeline" AND duration_ms > 60000 AND event:"agent_session_completed"
```

---

## 10. Prometheus Query Reference

Open http://localhost:9090 and paste these in the Expression box.

### Service Health

```promql
# Are all pipeline services up? (1=up, 0=down)
up{job=~"genai-pipeline|ml-pipeline|agentic-pipeline|kafka-exporter"}

# How long has each service been up? (seconds)
process_uptime_seconds
```

### GenAI Deep Dive

```promql
# Request rate by model (which model is most used?)
sum by (model) (rate(genai_requests_total[5m]))

# Error breakdown by type
sum by (model, status) (rate(genai_requests_total[5m]))

# Latency percentiles (P50, P90, P99)
histogram_quantile(0.50, rate(genai_request_duration_seconds_bucket[5m]))
histogram_quantile(0.90, rate(genai_request_duration_seconds_bucket[5m]))
histogram_quantile(0.99, rate(genai_request_duration_seconds_bucket[5m]))

# Total tokens produced today (input vs output)
sum by (type) (genai_tokens_total)

# Rate limit hit rate per second
rate(genai_rate_limit_hits_total[5m])
```

### Agentic AI Deep Dive

```promql
# Session success rate
rate(agent_sessions_total{status="completed"}[10m])
/ rate(agent_sessions_total[10m])

# What fraction of sessions hit max_steps?
rate(agent_max_steps_reached_total[10m])
/ rate(agent_sessions_total[10m])

# Tool failure rate by tool name
rate(agent_tool_calls_total{status="error"}[5m])
/ rate(agent_tool_calls_total[5m])

# Average steps per session (should be 3-6)
increase(agent_steps_total[1h])
/ increase(agent_sessions_total{status="completed"}[1h])

# P95 session duration
histogram_quantile(0.95, rate(agent_session_duration_seconds_bucket[10m]))
```

### ML Pipeline Deep Dive

```promql
# Current model accuracy (Gauge — no rate needed)
ml_model_accuracy

# Data drift scores — all features
ml_data_drift_score

# Features with drift above threshold
ml_data_drift_score > 0.3

# Prediction confidence percentiles
histogram_quantile(0.10, rate(ml_prediction_confidence_bucket[10m]))
histogram_quantile(0.50, rate(ml_prediction_confidence_bucket[10m]))

# Feature missing rate
ml_feature_missing_rate
```

### Kafka Deep Dive

```promql
# Consumer lag per group and topic
kafka_consumergroup_lag

# Top 5 most lagging consumer groups
topk(5, kafka_consumergroup_lag)

# Is lag growing (positive) or shrinking (negative)?
deriv(kafka_consumergroup_lag[5m])

# Message production rate by topic
rate(kafka_topic_partition_current_offset[1m])
```

---

## 11. Alert Runbooks

### GenAIHighErrorRate (Critical)

**Fires when**: Error rate > 5% for 2 minutes

**Step-by-step diagnosis**:
```bash
# Step 1: Is the service running?
curl http://localhost:8001/health
docker compose ps genai-pipeline

# Step 2: What errors are being returned?
docker compose logs --tail=50 genai-pipeline | grep -E "ERROR|error_type"

# Step 3: What type of errors? (rate limit vs API down vs code bug)
# Prometheus: sum by (status) (rate(genai_requests_total[5m]))
# If "rate_limited" is high → Groq is throttling us
# If "error" is high → Groq API down or code bug

# Step 4: Check Groq API from inside the container
docker exec genai-pipeline curl -s https://api.groq.com/openai/v1/models \
  -H "Authorization: Bearer $GROQ_API_KEY" | python3 -m json.tool
```

**Resolution by error type**:
| Error Type | Fix |
|-----------|-----|
| `rate_limited` | Add exponential backoff + jitter. Consider model fallback (`llama-3.1-8b-instant` instead of 70b). |
| `APIError` | Check Groq status page. Implement circuit breaker. |
| Container crash | `docker compose restart genai-pipeline`. Check OOM: `docker stats` |

---

### AgentMaxStepsReached (Critical)

**Fires when**: > 0.1 sessions/min hitting max_steps for 2 minutes

**Step-by-step diagnosis**:
```bash
# Step 1: Find recent stuck sessions in Kibana
# Query: event:"agent_max_steps_reached" AND @timestamp > now-30m

# Step 2: Look at the task text
# Query: event:"agent_session_completed" AND status:"max_steps_reached"
# Check the "task" field — identify patterns

# Step 3: Trace the session in Jaeger
# http://localhost:16686 → agentic-pipeline → Min Duration: 30s
# Find sessions with many child spans (= many steps)
# Look for repeated tool_name in consecutive steps

# Step 4: Check for repeated tool calls
docker compose logs agentic-pipeline | \
  grep "agent_tool_call" | \
  awk '{print $0}' | \
  python3 -c "
import sys, json
calls = []
for line in sys.stdin:
    try:
        data = json.loads(line)
        if data.get('event') == 'agent_tool_call':
            calls.append((data.get('session_id'), data.get('tool_name'), data.get('step')))
    except: pass
for c in calls[-20:]: print(c)
"
```

**Resolution**:
- Temporarily reduce `max_steps` to 5 to limit blast radius
- Review tasks that trigger loops — add guard prompt: *"Have you already tried this?"*
- Add tool-call deduplication: track (tool_name, args) pairs; stop if repeated

---

### KafkaConsumerLagHigh (Critical)

**Fires when**: Consumer lag > 10,000 messages for 5 minutes

**Step-by-step diagnosis**:
```bash
# Step 1: Which consumer group and topic is lagging?
docker exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --all-groups
# Look for CONSUMER-ID="-" (disconnected consumer) or high LAG column

# Step 2: Are the consumers running?
docker compose ps
# If any pipeline service shows "Exit 1" → that's the broken consumer

# Step 3: Is the producer flooding the topic?
# Kafka UI: http://localhost:8080 → Topics → check "Messages In Per Second"
# If suddenly 100x higher → upstream is producing too fast

# Step 4: Is consumer processing slow?
# Prometheus: rate(kafka_message_processing_duration_seconds_sum[5m])
#           / rate(kafka_message_processing_duration_seconds_count[5m])
# Compare to baseline — if 10x slower, find the slow operation

# Step 5: Emergency — skip stale messages
docker exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group ml-pipeline-consumer \
  --topic pipeline-events \
  --reset-offsets --to-latest \
  --execute
```

---

### MLDataDriftDetected (Warning)

**Fires when**: `ml_data_drift_score > 0.3` for 10 minutes

**Step-by-step diagnosis**:
```bash
# Step 1: Which features are drifting?
# Prometheus: ml_data_drift_score
# Sort by value — highest drift = most important to investigate

# Step 2: Check Logstash anomaly tags in Kibana
# Query: tags:("drifted_feature") OR anomaly:*

# Step 3: Was there a recent upstream data pipeline change?
# Check data engineering team's deployment log

# Step 4: Run BigQuery post-mortem query to quantify drift impact
# See bigquery/outage_queries.sql — Query 6 (low confidence predictions)
```

**Resolution**:
1. If drift is in `transaction_amount` only → likely a legitimate seasonal/business change
2. If drift is in multiple features → upstream data pipeline bug
3. Trigger model retraining on recent data
4. Until retrained: add business-logic guardrails (flag high-drift inputs for manual review)

---

## 12. Kafka Monitoring

### Understanding Consumer Lag (the most important Kafka metric)

```
Kafka Topic: pipeline-events
┌──────────────────────────────────────────┐
│  Partition 0                              │
│  [msg1] [msg2] [msg3] [msg4] [msg5]      │
│                              ↑            │
│                          End Offset: 5    │
└──────────────────────────────────────────┘
                    ↑
         Consumer has read up to msg3
         Consumer Offset: 3

LAG = End Offset - Consumer Offset = 5 - 3 = 2

Healthy:   LAG < 100 (consumer keeping up)
Warning:   LAG > 1,000 (consumer falling behind)
Critical:  LAG > 10,000 (pipeline severely delayed)
Disaster:  LAG growing faster than consumer processes (never catches up)
```

### Essential Kafka CLI Commands

```bash
# List all topics
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Create a topic (3 partitions for parallelism)
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic my-topic \
  --partitions 3 --replication-factor 1

# Check all consumer groups and their lag
docker exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --all-groups

# Produce a test message
echo '{"test": "message"}' | docker exec -i kafka \
  kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic genai-events

# Consume messages (inspect topic contents)
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic genai-events \
  --from-beginning \
  --max-messages 10

# Get partition end offsets (how many total messages)
docker exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic genai-events \
  --time -1

# Delete a topic (if needed for cleanup)
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete --topic test-topic
```

### Kafka UI (http://localhost:8080)

Navigate the Kafka UI to:
- **Brokers**: Health of the Kafka cluster
- **Topics**: Message throughput, partition details, message preview
- **Consumer Groups**: Live lag per group per partition — this is your primary monitoring view
- **Schema Registry**: (if using Avro/Protobuf schemas)

---

## 13. BigQuery & Multi-Cloud Log Analytics

This stack supports three cloud providers for historical log analytics. SQL files live in separate folders per provider. The default is **Google BigQuery** backed by GCS.

| Cloud | Query Engine | Object Store | SQL folder |
|---|---|---|---|
| Google Cloud (default) | BigQuery | GCS (`gs://`) | `bigquery/` |
| AWS | Athena | S3 (`s3://`) | `athena/` |
| Azure | Synapse Serverless SQL | ADLS Gen2 | `azure/` |

**Cost**: BigQuery charges ~$5 per TB scanned. Use `WHERE timestamp >=` filters on every query to avoid full-table scans.

### Google BigQuery Setup (already done if you followed Quick Start)

```bash
# 1. Authenticate
gcloud auth application-default login

# 2. Create dataset
bq mk --dataset --location=US observibility-monitoring:pipeline_observability

# 3. Create external tables (backed by GCS)
bq query --project_id=observibility-monitoring --use_legacy_sql=false < bigquery/setup.sql
```

### Running Post-Mortem Queries in Python

```python
from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project="observibility-monitoring")

# Error rate over an incident window
query = """
SELECT
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(TIMESTAMP(timestamp), HOUR),
    INTERVAL (EXTRACT(MINUTE FROM TIMESTAMP(timestamp)) DIV 15 * 15) MINUTE
  )                                                     AS time_bucket,
  service,
  COUNT(*)                                              AS total_requests,
  COUNTIF(level = 'ERROR')                              AS errors,
  ROUND(100.0 * COUNTIF(level = 'ERROR') / COUNT(*), 2) AS error_rate_pct,
  ROUND(APPROX_QUANTILES(latency_ms, 100)[OFFSET(99)], 1) AS p99_latency_ms
FROM `observibility-monitoring.pipeline_observability.genai_logs`
WHERE timestamp >= '2025-01-15T10:00:00'
  AND timestamp <= '2025-01-15T12:00:00'
GROUP BY 1, 2
ORDER BY 1 DESC
"""

df = client.query(query).to_dataframe()
print(df.to_string())
```

### BigQuery Cost-Saving Tips

```sql
-- ALWAYS filter on timestamp first (limits bytes scanned)
WHERE timestamp >= '2025-01-15T10:00:00'

-- Use APPROX_QUANTILES not exact percentiles (faster, ~1% error)
APPROX_QUANTILES(latency_ms, 100)[OFFSET(99)]  -- p99, good enough for incidents

-- LIMIT does NOT reduce bytes scanned — use WHERE clauses
-- Bad:  SELECT * FROM genai_logs LIMIT 100          (scans full table)
-- Good: SELECT * FROM genai_logs WHERE timestamp >= '...' LIMIT 100

-- Preview query cost before running (BigQuery console shows estimate)
-- Or check bytes via: bq query --dry_run --use_legacy_sql=false "SELECT ..."
```

### AWS Athena (alternative)

Uncomment the AWS block in `.env` and run:

```bash
aws configure   # enter Access Key, Secret, Region

aws athena start-query-execution \
  --query-string "CREATE DATABASE IF NOT EXISTS pipeline_observability" \
  --result-configuration "OutputLocation=s3://your-bucket/athena-results/"

# Then paste athena/setup.sql into the Athena console
```

See `athena/outage_queries.sql` for 10 Presto-SQL incident queries.

### Azure Synapse Serverless SQL (alternative)

Uncomment the Azure block in `.env`, then run `azure/setup.sql` in the Synapse Studio SQL editor. See `azure/outage_queries.sql` for T-SQL equivalents of all 10 queries.

See all 10 production-ready incident investigation queries in `bigquery/outage_queries.sql`.

---

## 14. Distributed Tracing with Jaeger

Jaeger UI: http://localhost:16686

### Anatomy of a Trace

A **trace** is a tree of **spans**. Each span represents one unit of work.

```
Trace ID: abc-123
│
├── SPAN: agent_session           (duration: 45.2s)   ← ROOT SPAN
│   ├── SPAN: agent_step_1       (duration: 2.1s)
│   │   └── SPAN: llm_inference  (duration: 1.8s)
│   │       Attributes:
│   │         llm.model = "llama-3.3-70b-versatile"
│   │         llm.input_tokens = 234
│   │         llm.latency_ms = 1798.4
│   │
│   ├── SPAN: agent_step_2       (duration: 12.8s)    ← SLOW STEP
│   │   ├── SPAN: llm_inference  (duration: 1.5s)
│   │   └── SPAN: tool_web_search (duration: 11.2s)  ← BOTTLENECK
│   │       Status: ERROR
│   │       Error: "Search service timeout"
│   │
│   └── SPAN: agent_step_3       (duration: 0.9s)
│       └── SPAN: llm_inference  (duration: 0.8s)
│           Attributes:
│             llm.finish_reason = "stop"              ← Final answer
```

### How to Find the Bottleneck

1. Open Jaeger: http://localhost:16686
2. **Service**: Select `agentic-pipeline` (or `genai-pipeline`)
3. **Operation**: Leave blank or select `agent_session`
4. **Min Duration**: Type `5s` to filter for slow traces
5. Click **Find Traces** → sort by Duration (slowest first)
6. Click the slowest trace
7. Look for the widest bar (longest span) — that's your bottleneck

### Correlating a Trace with Logs

```bash
# From a Jaeger trace, copy the "request.id" attribute value
# Example: "req-1705312425123"

# Then find the exact logs for that request:
# In Kibana: request_id:"req-1705312425123"
# In Loki: {service="genai-pipeline"} | json | request_id="req-1705312425123"
```

This is the power of **correlation IDs** — they let you jump between metrics, logs, and traces for the same exact request.

---

## 15. Hands-On Labs

### Prerequisites for Labs

```bash
# Install lab dependencies
pip install httpx aiohttp

# Make sure all services are running
docker compose ps

# Check services respond
curl -s http://localhost:8001/health | python3 -m json.tool
curl -s http://localhost:8002/health | python3 -m json.tool
curl -s http://localhost:8003/health | python3 -m json.tool
```

---

### Lab 1: LLM Rate Limit Storm

**Objective**: Experience, observe, and diagnose a Groq API rate limit incident.

```bash
# Start the rate limit scenario (runs for 3 minutes)
python src/outage_simulator/simulator.py --scenario llm_rate_limit --duration 180
```

**What to do while it runs**:
1. Open Grafana (http://localhost:3000) → GenAI Dashboard
2. Watch **Rate Limit Hits** panel start spiking
3. Watch **Error Rate** panel climb above 5%
4. Open Prometheus: query `rate(genai_rate_limit_hits_total[5m])`
5. Open AlertManager (http://localhost:9093) — watch `GenAIRateLimitHigh` fire
6. Open Kibana → filter: `event:"llm_rate_limited"`

**Lab questions to answer**:
- At what RPS did rate limits start? (Check request rate when rate limits appeared)
- Which model is hitting limits more? (Check by `model` label)
- How long before the alert fired? (Check AlertManager `for` duration)
- What would you implement to fix this? (Exponential backoff? Model fallback?)

---

### Lab 2: Agent Infinite Loop

**Objective**: Detect agents stuck in reasoning loops and find root cause.

```bash
python src/outage_simulator/simulator.py --scenario agent_loop --duration 240
```

**What to do**:
1. Open Prometheus: `rate(agent_max_steps_reached_total[5m])`
2. Watch the counter increase over time
3. Open Jaeger → `agentic-pipeline` → Min Duration: 20s
4. Find a trace with many child spans (many `agent_step_N` spans)
5. In Kibana: `event:"agent_tool_call" AND step:>5` — see if same tool is called repeatedly
6. Identify which task prompts triggered the loop behavior

**Lab questions**:
- How many steps did the stuck sessions take?
- Was the same tool being called with the same arguments?
- What is the session duration at max_steps=3 vs max_steps=10?

---

### Lab 3: ML Data Drift

**Objective**: Identify which features drifted and understand the downstream impact.

```bash
python src/outage_simulator/simulator.py --scenario ml_data_drift --duration 180
```

**What to do**:
1. Open Prometheus: `ml_data_drift_score` — watch scores climb
2. Which features crossed 0.3? (Look at the `feature_name` label)
3. Open Kibana: `anomaly:* AND service:"ml-pipeline"`
4. Check prediction confidence: `histogram_quantile(0.50, rate(ml_prediction_confidence_bucket[10m]))`
5. Does drift correlate with lower confidence?

**The Drift Injection** (inspect the simulator code):
```python
# simulator.py injects these drifted values:
transaction_amount = random.uniform(50000, 500000)  # 500x normal
user_age_days = random.randint(1, 30)                # All new accounts
tx_count_24h = random.randint(50, 200)               # 50x normal frequency
is_international = True                              # 100% international
device_trust_score = random.uniform(0.0, 0.2)        # All low-trust
```

**Lab question**: With this drift pattern, does fraud probability increase or decrease? Why?

---

### Lab 4: Kafka Consumer Lag

**Objective**: Create consumer lag, monitor it, then recover.

```bash
# Step 1: Stop the ML pipeline (creates lag in pipeline-events topic)
docker compose stop ml-pipeline

# Step 2: Watch lag build up
watch -n 3 'curl -s "http://localhost:9090/api/v1/query?query=kafka_consumergroup_lag" \
  | python3 -c "import sys,json; data=json.load(sys.stdin); \
    [print(r[\"metric\"][\"consumergroup\"] + \": lag=\" + r[\"value\"][1]) \
     for r in data[\"data\"][\"result\"]]" 2>/dev/null || echo "No data yet"'

# Step 3: Also check Kafka UI
open http://localhost:8080
# Navigate to Consumer Groups → see lag per partition

# Step 4: Restart the consumer and watch lag drain
docker compose start ml-pipeline

# Step 5: Monitor recovery — lag should drain within minutes
# Prometheus: deriv(kafka_consumergroup_lag[5m])
# Negative value = catching up (good!)
```

**Lab question**: If lag is at 50,000 messages and the consumer processes 100 msg/s, how long to catch up?

---

### Lab 5: Full Cascade Failure (Advanced)

**Objective**: Diagnose a multi-service incident with overlapping alerts.

```bash
python src/outage_simulator/simulator.py --scenario cascade_failure --duration 300
```

**Challenge**: Multiple alerts fire simultaneously. Your job:
1. Note the EXACT time each alert fires (check AlertManager)
2. Determine which was the FIRST alert
3. Determine if they are causally linked or independent
4. Identify the root cause service
5. Write a 5-line incident summary:
   - What broke
   - When it started
   - What was the blast radius
   - What fixed it
   - What prevents recurrence

---

## 16. SLI, SLO, SLA Definitions

### Definitions

| Term | Meaning | Example |
|------|---------|---------|
| **SLI** (Service Level Indicator) | A measurable metric | P99 latency in milliseconds |
| **SLO** (Service Level Objective) | Target value for an SLI | P99 latency < 10s for 99.9% of requests |
| **SLA** (Service Level Agreement) | Contractual commitment | "If SLO is missed, customer gets refund" |
| **Error Budget** | Allowed failure quota | SLO=99.9% → 0.1% = 43.8 min/month can fail |

### This Stack's SLOs

| Service | SLI | Target | Alert Threshold |
|---------|-----|--------|-----------------|
| GenAI Pipeline | P99 Latency | < 10s | > 10s for 3 min |
| GenAI Pipeline | Error Rate | < 5% | > 5% for 2 min |
| GenAI Pipeline | Availability | > 99.9% | Down > 1 min |
| Agentic Pipeline | Session Success Rate | > 95% | < 95% for 5 min |
| Agentic Pipeline | P95 Session Duration | < 120s | > 120s for 5 min |
| ML Pipeline | P99 Inference Latency | < 2s | > 2s for 3 min |
| ML Pipeline | Model Accuracy | > 80% | < 80% for 15 min |
| Kafka | Consumer Lag | < 10,000 | > 10k for 5 min |

### Error Budget Calculation

```
Monthly error budget for GenAI pipeline (SLO: 99.5% success):
  Total minutes/month = 30 * 24 * 60 = 43,200 minutes
  Error budget = 0.5% × 43,200 = 216 minutes of downtime/month

If an incident lasts 45 minutes:
  Remaining budget = (216 - 45) / 216 = 79% remaining

If you've burned 80% of your error budget by mid-month:
  → Freeze non-critical deployments for rest of month
  → Focus only on reliability improvements
```

---

## 17. Production Best Practices

### Metrics

```
✅ Use HISTOGRAMS for latency — averages hide tail latency
   Bad:  latency_sum / latency_count  (average)
   Good: histogram_quantile(0.99, rate(latency_bucket[5m]))

✅ Apply RED method to every service (Rate, Errors, Duration)

✅ Set alert thresholds based on SLO breach, not arbitrary numbers
   Bad:  alert when CPU > 80%
   Good: alert when error_rate > SLO threshold for N minutes

✅ Use recording rules for expensive/frequently-used queries
   # In prometheus.yml:
   record: job:genai_error_rate:rate5m
   expr: rate(genai_requests_total{status="error"}[5m]) / rate(genai_requests_total[5m])

✅ Label consistently across all services
   Required labels: service, environment, version

✅ Track LLM cost (tokens) as first-class business metric
```

### Logging

```
✅ ALWAYS use structured JSON logging — never plain text in production
   Bad:  logger.info(f"Request {req_id} completed in {latency}ms")
   Good: logger.info("request_completed", request_id=req_id, latency_ms=latency)

✅ Include request_id/trace_id in EVERY log line for correlation

✅ Log at ERROR only for failures requiring human action
   Not for: 4xx user errors (those are info), expected retries

✅ Always include latency_ms in operation completion logs

✅ Sample DEBUG logs in production (100% = disk fills fast)
   Example: only log full prompts for 1% of requests

✅ Retention strategy: 7 days hot (Elasticsearch), 90 days cold (S3/Athena)
```

### Distributed Tracing

```
✅ Propagate trace context across service boundaries
   For HTTP:  W3C Trace Context headers
   For Kafka: inject trace context into message headers

✅ Sample traces: 100% errors, 10% successful requests (saves cost)

✅ Use meaningful span names (describe the operation, not the code)
   Bad:  "handler" or "process"
   Good: "llm_inference" or "agent_step_fraud_check"

✅ Add business context as span attributes
   span.set_attribute("llm.model", model_name)
   span.set_attribute("llm.input_tokens", prompt_tokens)
   span.set_attribute("agent.step_number", step)
```

### Alerting

```
✅ Every alert MUST have a runbook URL in its annotation
   annotations:
     runbook_url: "https://wiki.internal/runbooks/genai-rate-limit"

✅ Alert on SYMPTOMS (high error rate) not CAUSES (Groq API down)
   Bad:  "Alert when external API returns 429"
   Good: "Alert when our error rate exceeds 5%"

✅ Never alert on something you can't act on immediately

✅ Use inhibition rules to suppress downstream alerts
   If ServiceDown fires, suppress ErrorRate for that service
   (The service being down is the root cause; error rate is a symptom)

✅ Test your alerts! Use the outage simulator to verify they fire correctly
```

### GenAI / Agentic AI Specific

```
✅ Always set max_tokens — unbounded LLM calls = unbounded cost
   Every prompt: max_tokens=512 or adjusted per use case

✅ Always set max_steps for agents — no upper limit = infinite loop risk
   Every agent: max_steps=10 (adjust based on task complexity)

✅ Implement circuit breakers for LLM APIs
   When rate_limit_hits_total rate > X: circuit open → fail fast
   After 60s: circuit half-open → try one request

✅ Log prompts (sampled) — you cannot reproduce bugs without them
   Sample rate: 1-5% of successful requests, 100% of errors

✅ Monitor context window utilization actively
   > 85% → warn (silent truncation degrades answer quality)

✅ Use the cheapest/fastest model that meets quality requirements
   Default to: llama-3.1-8b-instant
   Escalate to: llama-3.3-70b-versatile only when needed

✅ Track per-session LLM cost, not just total
   Cost = (input_tokens × $0.05/1M) + (output_tokens × $0.08/1M)
   Set per-session budget limits to prevent runaway
```

---

## Service URLs Quick Reference

| Service | URL | Purpose |
|---------|-----|---------|
| **Grafana** | http://localhost:3000 | Main dashboards (admin/admin123) |
| **Prometheus** | http://localhost:9090 | Raw metric queries |
| **AlertManager** | http://localhost:9093 | Active alerts & routing |
| **Kibana** | http://localhost:5601 | Log search & analysis |
| **Jaeger** | http://localhost:16686 | Distributed traces |
| **Kafka UI** | http://localhost:8080 | Topic & consumer group management |
| **GenAI API** | http://localhost:8001/docs | Swagger for GenAI service |
| **ML API** | http://localhost:8002/docs | Swagger for ML service |
| **Agent API** | http://localhost:8003/docs | Swagger for Agentic service |

## Shutting Down

```bash
# Stop all services (keep data volumes — logs/metrics preserved)
docker compose down

# Stop and DELETE all data (full clean slate)
docker compose down -v

# Restart a single service after code changes
docker compose up -d --build genai-pipeline

# View resource usage
docker stats
```

---

*Stack: Prometheus · Grafana · Loki · AlertManager · Elasticsearch · Kibana · Logstash · Jaeger · OpenTelemetry · Kafka · Groq (llama-3.1-8b-instant / llama-3.3-70b-versatile) · Python 3.13 · FastAPI · Scikit-learn · Google BigQuery · GCS — also supports AWS Athena · Azure Synapse*

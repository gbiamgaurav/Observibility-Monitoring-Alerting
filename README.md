# Monitoring a GenAI Application in Production

You've built and deployed a GenAI application. Now you need to know:
- Is it working?
- Is it slow?
- Is it costing too much?
- When it breaks — where exactly and why?

This stack gives you the answer to all four questions using real production tools.

---

## What's Running

This stack monitors three AI services:

| Service | What it does | Port |
|---|---|---|
| **GenAI Pipeline** | Calls Groq LLM API (llama-3.1-8b-instant) to answer prompts | 8001 |
| **Agentic Pipeline** | ReAct agent that reasons step-by-step using tools | 8003 |
| **ML Pipeline** | Fraud detection model (sklearn Random Forest) | 8002 |

---

## Quick Start

```bash
# 1. Add your Groq API key to .env
echo "GROQ_API_KEY=gsk_your_key_here" >> .env

# 2. Start everything
docker compose up -d

# 3. Open your monitoring dashboard
open http://localhost:3000   # Grafana — login: admin / admin123
```

That's it. Metrics, logs, traces, and alerts all start flowing automatically.

---

## The Four Tools You'll Use Daily

### 1. Grafana — `http://localhost:3000`
**Your main screen.** Shows real-time graphs of every metric from all services.

→ **AI Pipeline Dashboard:** `http://localhost:3000/d/e27ee566-23a2-4407-817b-87a746b31150`

### 2. Kibana — `http://localhost:5601`
**Search your logs.** Find the exact error message for any failure.

→ First time: create index pattern `pipeline-logs-*` with time field `@timestamp`

### 3. Jaeger — `http://localhost:16686`
**Trace a request end-to-end.** See exactly which step was slow or failed.

### 4. Prometheus — `http://localhost:9090`
**Raw metrics and alerts.** Check `/alerts` to see what's broken right now.

---

## The Metrics That Matter in Production

These are the metrics you must watch for any GenAI application. They are already being collected and displayed in Grafana.

---

### Reliability Metrics — "Is it working?"

#### 1. Error Rate
```
What it is:   % of LLM requests that fail
Where to see: Grafana → "GenAI Request Rate" panel (red line = errors)
Prometheus:   rate(genai_requests_total{status="error"}[5m]) / rate(genai_requests_total[5m]) * 100
Alert:        Fires when error rate > 5% for 2 minutes
Healthy:      < 1%
```

#### 2. Request Rate
```
What it is:   How many requests/second your GenAI service is handling
Where to see: Grafana → "GenAI Request Rate" panel
Prometheus:   rate(genai_requests_total[5m])
Why it matters: A sudden drop to 0 means the service is dead
Healthy:      Steady line matching your expected load
```

#### 3. Service Availability (Up/Down)
```
What it is:   Is the service running and reachable?
Where to see: Prometheus → http://localhost:9090 → type: up
Prometheus:   up{job="genai-pipeline"}   →  1 = running, 0 = down
Alert:        Fires if down for > 1 minute
```

---

### Performance Metrics — "Is it fast enough?"

#### 4. P99 Latency (most important latency metric)
```
What it is:   The slowest 1% of requests — what your worst-case users experience
Where to see: Grafana → "GenAI Latency P99 (ms)" panel
Prometheus:   histogram_quantile(0.99, rate(genai_request_duration_seconds_bucket[5m])) * 1000
Alert:        Fires when P99 > 10 seconds for 3 minutes
Healthy:      < 3s for 8b models, < 10s for 70b models

Why P99 and not average?
  Average = 1.2s looks fine
  P99 = 28s means 1 in 100 users waits 28 seconds — that's broken
```

#### 5. P50 Latency (median — what typical users experience)
```
What it is:   Half of requests are faster than this, half are slower
Prometheus:   histogram_quantile(0.50, rate(genai_request_duration_seconds_bucket[5m])) * 1000
Healthy:      < 1s for 8b models
```

#### 6. Queue Depth
```
What it is:   Requests waiting to be processed
Where to see: Grafana → "GenAI Queue Depth" panel (stat)
Prometheus:   genai_queue_depth
Alert:        Growing queue = service can't keep up with load
Healthy:      0–5 (spikes OK, sustained growth = problem)
```

---

### Cost Metrics — "How much is it spending?"

#### 7. Token Usage Rate
```
What it is:   How many tokens/second your LLM is consuming (= money)
Where to see: Grafana → "GenAI Token Usage Rate" panel
Prometheus:   rate(genai_tokens_total[1m])
              Split by type="input" and type="output"

Why output tokens cost more:
  Input tokens  = what you send to the model
  Output tokens = what the model generates  ← costs 2x more on most APIs
  Output tokens growing faster than input = model is rambling / agent is looping

Alert:  Fires if output tokens exceed 100,000/hour (possible runaway)
```

#### 8. Context Window Utilization
```
What it is:   How much of the model's memory (context window) you're using
Where to see: Grafana → "Context Window Utilization" gauge
Prometheus:   genai_context_window_utilization
Alert:        Fires when > 85%

What happens above 85%:
  The model silently drops the oldest parts of the conversation.
  Your agent forgets what it was doing. Answers become wrong.
Healthy:      < 70%
```

---

### LLM API Health Metrics — "Is the external API behaving?"

#### 9. Rate Limit Hits
```
What it is:   How many times Groq (or any LLM provider) returned HTTP 429 (Too Many Requests)
Where to see: Grafana → "Rate Limit Hits" stat panel (red = bad)
Prometheus:   rate(genai_rate_limit_hits_total[5m])
Alert:        Fires when hitting > 0.5 rate limits/second for 2 minutes

What to do:
  - Add exponential backoff with jitter
  - Switch to a smaller/faster model (8b instead of 70b)
  - Upgrade your API tier
Healthy:      0 (should always be zero)
```

---

### Agentic AI Specific Metrics — "Is the agent behaving?"

#### 10. Session Success Rate
```
What it is:   % of agent sessions that complete successfully
Prometheus:   rate(agent_sessions_total{status="completed"}[10m]) / rate(agent_sessions_total[10m]) * 100
Healthy:      > 95%
```

#### 11. Max Steps Reached (Loop Detection)
```
What it is:   Agent hit the step limit without finishing — likely stuck in a loop
Where to see: Grafana → "Rate Limit Hits" (also shows agent loop events)
Prometheus:   rate(agent_max_steps_reached_total[10m])
Alert:        Fires when > 0.1 sessions/min are looping

What it means:
  Agent is calling the same tool repeatedly with the same arguments.
  It never makes progress, burns tokens, and times out.
Healthy:      0
```

#### 12. Average Steps Per Session
```
What it is:   How many reasoning steps each agent session takes on average
Prometheus:   increase(agent_steps_total[10m]) / increase(agent_sessions_total[10m])
Healthy:      3–6 steps
Warning:      Consistently > 8 steps = agent prompts need tuning
```

#### 13. Tool Call Failure Rate
```
What it is:   % of tool calls (web search, calculator, APIs) that fail
Prometheus:   rate(agent_tool_calls_total{status="error"}[5m]) / rate(agent_tool_calls_total[5m]) * 100
Alert:        Fires when any tool fails > 10% for 2 minutes
Healthy:      < 2%
```

---

### All Metrics at a Glance

| Metric | Healthy | Warning | Critical |
|---|---|---|---|
| Error Rate | < 1% | 1–5% | > 5% |
| P99 Latency | < 3s | 3–10s | > 10s |
| Rate Limit Hits | 0 | Any | > 0.5/s sustained |
| Queue Depth | 0–5 | 5–50 | > 50 growing |
| Context Window | < 70% | 70–85% | > 85% |
| Token Rate | Normal | 2x baseline | 10x baseline |
| Session Success | > 95% | 85–95% | < 85% |
| Agent Loops | 0 | Any | > 0.1/min |
| Tool Failure | < 2% | 2–10% | > 10% |

---

## How to Find an Error (Step by Step)

When something goes wrong, follow this exact sequence:

```
Step 1 → http://localhost:9090/alerts
         Look for RED alerts. Note which service and what fired.

Step 2 → http://localhost:3000
         Open the AI Pipeline dashboard.
         Change time range to "Last 15 minutes".
         Find which panel spiked at the same time the alert fired.

Step 3 → http://localhost:16686  (Jaeger)
         Select the broken service.
         Set Min Duration to "5s".
         Click Find Traces → click the slowest/failed trace.
         The RED span = where it broke.

Step 4 → http://localhost:5601  (Kibana)
         Copy the request_id from the Jaeger trace.
         Search: request_id: "req-..."
         Read the actual error message.
```

---

## Log Queries — Finding Errors in Kibana

Go to `http://localhost:5601` → Discover → select **Pipeline Logs**

```
All errors:
  level: ERROR

Errors from GenAI only:
  service: "genai-pipeline" AND level: ERROR

Rate limit errors (Groq throttling):
  event: "llm_rate_limited"

Slow requests over 5 seconds:
  latency_ms > 5000

Agent sessions that looped:
  status: "max_steps_reached"

Find a specific request:
  request_id: "req-1780161814533"

Errors in the last hour:
  level: ERROR AND @timestamp > now-1h
```

---

## Alerts Configured

These alerts fire automatically and can notify Slack or PagerDuty (configure in `.env`).

| Alert | Fires When | Severity |
|---|---|---|
| `GenAIHighErrorRate` | Error rate > 5% for 2 min | Critical |
| `GenAIHighLatency` | P99 latency > 10s for 3 min | Warning |
| `GenAIRateLimitHigh` | > 0.5 rate limits/sec for 2 min | Warning |
| `GenAIPipelineDead` | Zero requests for 5 min | Critical |
| `GenAITokenCostSpike` | Output tokens > 100k/hour | Warning |
| `GenAIContextWindowHigh` | Context usage > 85% | Warning |
| `AgentMaxStepsReached` | > 0.1 loops/min for 2 min | Critical |
| `AgentToolFailureRate` | Tool failures > 10% for 2 min | Warning |
| `AgentSessionLatencyHigh` | P95 session > 120s for 5 min | Warning |

Configure notifications in `.env`:
```bash
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK
PAGERDUTY_SERVICE_KEY=your_pagerduty_key
```

---

## Production Setup by Cloud

### Google Cloud (GCP)
```bash
# 1. Authenticate
gcloud auth application-default login

# 2. Configure .env
GCP_PROJECT_ID=your-project-id
GCS_LOG_BUCKET=gs://your-bucket/pipeline-logs/

# 3. Create BigQuery tables for historical analysis
bq mk --dataset your-project:pipeline_observability
bq query --use_legacy_sql=false < bigquery/setup.sql

# 4. Deploy managed equivalents
Prometheus  → Google Managed Service for Prometheus
Grafana     → Google Managed Grafana
Kafka       → Google Cloud Pub/Sub
Logs        → Cloud Logging + BigQuery
Traces      → Cloud Trace
```

### AWS
```bash
# 1. Authenticate
aws configure

# 2. Configure .env
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-east-1
PIPELINE_LOGS_S3=s3://your-bucket/pipeline-logs/

# 3. Deploy managed equivalents
Prometheus  → Amazon Managed Prometheus (AMP)
Grafana     → Amazon Managed Grafana (AMG)
Kafka       → Amazon MSK
Logs        → CloudWatch Logs + Athena (SQL in athena/)
Traces      → AWS X-Ray
```

### Azure
```bash
# 1. Authenticate
az login

# 2. Configure .env
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-secret
AZURE_STORAGE_ACCOUNT=yourstorageaccount

# 3. Deploy managed equivalents
Prometheus  → Azure Monitor Managed Prometheus
Grafana     → Azure Managed Grafana
Kafka       → Azure Event Hubs (Kafka-compatible)
Logs        → Azure Monitor Logs + Synapse (SQL in azure/)
Traces      → Azure Application Insights
```

---

## All Service URLs

| Tool | URL | Login |
|---|---|---|
| Grafana (dashboards) | http://localhost:3000 | admin / admin123 |
| Prometheus (metrics) | http://localhost:9090 | — |
| Alertmanager | http://localhost:9093 | — |
| Kibana (logs) | http://localhost:5601 | — |
| Jaeger (traces) | http://localhost:16686 | — |
| Kafka UI | http://localhost:8080 | — |
| GenAI API | http://localhost:8001/docs | — |
| Agentic API | http://localhost:8003/docs | — |
| ML API | http://localhost:8002/docs | — |

## Stop / Start

```bash
docker compose up -d        # start everything
docker compose down         # stop, keep data
docker compose down -v      # stop, delete all data
docker compose restart genai-pipeline   # restart one service
docker compose logs -f genai-pipeline   # follow logs live
```

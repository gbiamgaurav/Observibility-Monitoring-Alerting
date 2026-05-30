-- ═══════════════════════════════════════════════════════════════
-- Google BigQuery Setup — External Tables over GCS Log Buckets
-- ═══════════════════════════════════════════════════════════════
-- Dataset 'pipeline_observability' must already exist.
-- Run: bq mk --dataset --location=US observibility-monitoring:pipeline_observability
-- ─────────────────────────────────────────────────────────────────


-- ─────────────────────────────────────────────────────────────────
-- GenAI Pipeline Logs
-- ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE EXTERNAL TABLE `observibility-monitoring.pipeline_observability.genai_logs` (
  timestamp        STRING,
  service          STRING,
  level            STRING,
  event            STRING,
  request_id       STRING,
  model            STRING,
  input_tokens     INT64,
  output_tokens    INT64,
  latency_ms       FLOAT64,
  finish_reason    STRING,
  ctx_utilization  FLOAT64,
  error_type       STRING,
  error            STRING,
  environment      STRING
)
OPTIONS (
  format                = 'NEWLINE_DELIMITED_JSON',
  uris                  = ['gs://observibility-monitoring/pipeline-logs/genai/*.json'],
  ignore_unknown_values = TRUE
);


-- ─────────────────────────────────────────────────────────────────
-- Agentic Pipeline Logs
-- ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE EXTERNAL TABLE `observibility-monitoring.pipeline_observability.agent_logs` (
  timestamp   STRING,
  service     STRING,
  level       STRING,
  event       STRING,
  session_id  STRING,
  agent_name  STRING,
  step        INT64,
  tool_name   STRING,
  steps_taken INT64,
  duration_ms FLOAT64,
  status      STRING,
  error       STRING,
  environment STRING
)
OPTIONS (
  format                = 'NEWLINE_DELIMITED_JSON',
  uris                  = ['gs://observibility-monitoring/pipeline-logs/agentic/*.json'],
  ignore_unknown_values = TRUE
);


-- ─────────────────────────────────────────────────────────────────
-- ML Pipeline Logs
-- ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE EXTERNAL TABLE `observibility-monitoring.pipeline_observability.ml_logs` (
  timestamp         STRING,
  service           STRING,
  level             STRING,
  event             STRING,
  transaction_id    STRING,
  is_fraud          BOOL,
  fraud_probability FLOAT64,
  confidence        FLOAT64,
  latency_ms        FLOAT64,
  model_version     STRING,
  error             STRING
)
OPTIONS (
  format                = 'NEWLINE_DELIMITED_JSON',
  uris                  = ['gs://observibility-monitoring/pipeline-logs/ml/*.json'],
  ignore_unknown_values = TRUE
);


-- ─────────────────────────────────────────────────────────────────
-- Kafka Consumer Offset Logs
-- Note: 'partition' is a reserved keyword — escaped with backticks
-- ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE EXTERNAL TABLE `observibility-monitoring.pipeline_observability.kafka_consumer_offsets` (
  timestamp       STRING,
  consumer_group  STRING,
  topic           STRING,
  `partition`     INT64,
  current_offset  INT64,
  log_end_offset  INT64,
  lag             INT64
)
OPTIONS (
  format                = 'NEWLINE_DELIMITED_JSON',
  uris                  = ['gs://observibility-monitoring/kafka-metrics/*.json'],
  ignore_unknown_values = TRUE
);

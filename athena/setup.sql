-- ═══════════════════════════════════════════════════════════════
-- AWS Athena Setup — Create External Tables over S3 Log Buckets
-- ═══════════════════════════════════════════════════════════════
-- Run these once to register your S3 log data as queryable tables.
-- Replace 's3://YOUR_BUCKET/...' with your actual S3 paths.
-- ─────────────────────────────────────────────────────────────────

-- 1. Create a dedicated database
CREATE DATABASE IF NOT EXISTS pipeline_observability
COMMENT 'Log analytics for AI pipeline observability';

USE pipeline_observability;

-- ─────────────────────────────────────────────────────────────────
-- GenAI Pipeline Logs
-- ─────────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS genai_logs (
    timestamp          STRING,
    service            STRING,
    level              STRING,
    event              STRING,           -- log event name, e.g. "llm_inference_success"
    request_id         STRING,
    model              STRING,
    input_tokens       BIGINT,
    output_tokens      BIGINT,
    latency_ms         DOUBLE,
    finish_reason      STRING,
    ctx_utilization    DOUBLE,
    error_type         STRING,
    error              STRING,
    environment        STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'true'
)
LOCATION 's3://YOUR_BUCKET/pipeline-logs/genai/'
TBLPROPERTIES (
    'has_encrypted_data' = 'false',
    'classification' = 'json'
);

-- ─────────────────────────────────────────────────────────────────
-- Agentic Pipeline Logs
-- ─────────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS agent_logs (
    timestamp          STRING,
    service            STRING,
    level              STRING,
    event              STRING,
    session_id         STRING,
    agent_name         STRING,
    step               INT,
    tool_name          STRING,
    steps_taken        INT,
    duration_ms        DOUBLE,
    status             STRING,
    error              STRING,
    environment        STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION 's3://YOUR_BUCKET/pipeline-logs/agentic/'
TBLPROPERTIES ('classification' = 'json');

-- ─────────────────────────────────────────────────────────────────
-- ML Pipeline Logs (partitioned by date for cost efficiency)
-- ─────────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS ml_logs (
    timestamp          STRING,
    service            STRING,
    level              STRING,
    event              STRING,
    transaction_id     STRING,
    is_fraud           BOOLEAN,
    fraud_probability  DOUBLE,
    confidence         DOUBLE,
    latency_ms         DOUBLE,
    model_version      STRING,
    error              STRING
)
PARTITIONED BY (
    year    STRING,
    month   STRING,
    day     STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION 's3://YOUR_BUCKET/pipeline-logs/ml/'
TBLPROPERTIES ('classification' = 'json');

-- Load partitions (run after adding new data)
MSCK REPAIR TABLE ml_logs;

-- ─────────────────────────────────────────────────────────────────
-- Kafka Consumer Offset Logs (exported from MSK / Confluent)
-- ─────────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS kafka_consumer_offsets (
    timestamp          STRING,
    consumer_group     STRING,
    topic              STRING,
    partition          INT,
    current_offset     BIGINT,
    log_end_offset     BIGINT,
    lag                BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://YOUR_BUCKET/kafka-metrics/'
TBLPROPERTIES ('classification' = 'json');

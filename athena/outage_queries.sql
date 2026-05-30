-- ═══════════════════════════════════════════════════════════════
-- Athena Outage Investigation Queries
-- ═══════════════════════════════════════════════════════════════
-- These queries let you do post-mortem analysis on production
-- outages by querying raw logs directly from S3.
-- Cost: Athena charges per TB scanned — use partition filters!
-- ─────────────────────────────────────────────────────────────────

USE pipeline_observability;


-- ─────────────────────────────────────────────────────────────────
-- 1. ERROR RATE OVER TIME (15-min buckets)
-- "When did the error spike start?"
-- ─────────────────────────────────────────────────────────────────
SELECT
    date_trunc('minute',
        from_iso8601_timestamp(timestamp)) -
        interval '0' minute
        + interval '15' minute
        * floor(minute(from_iso8601_timestamp(timestamp)) / 15) AS time_bucket,
    service,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) AS errors,
    ROUND(
        100.0 * SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS error_rate_pct,
    AVG(latency_ms) AS avg_latency_ms,
    APPROX_PERCENTILE(latency_ms, 0.99) AS p99_latency_ms
FROM genai_logs
WHERE timestamp >= '2025-01-01T00:00:00'   -- Replace with outage window
  AND timestamp <= '2025-01-02T00:00:00'
GROUP BY 1, 2
ORDER BY 1 DESC, error_rate_pct DESC;


-- ─────────────────────────────────────────────────────────────────
-- 2. TOP ERROR TYPES DURING OUTAGE
-- "What type of errors caused the spike?"
-- ─────────────────────────────────────────────────────────────────
SELECT
    error_type,
    error,
    COUNT(*) AS occurrences,
    MIN(timestamp) AS first_seen,
    MAX(timestamp) AS last_seen,
    ROUND(
        date_diff('minute',
            from_iso8601_timestamp(MIN(timestamp)),
            from_iso8601_timestamp(MAX(timestamp))
        ), 0
    ) AS duration_minutes
FROM genai_logs
WHERE level IN ('ERROR', 'CRITICAL')
  AND timestamp >= '2025-01-01T00:00:00'
  AND timestamp <= '2025-01-02T00:00:00'
GROUP BY 1, 2
ORDER BY occurrences DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────
-- 3. RATE LIMIT ANALYSIS — Was Groq throttling us?
-- ─────────────────────────────────────────────────────────────────
SELECT
    date_trunc('minute', from_iso8601_timestamp(timestamp)) AS minute,
    model,
    COUNT(*) AS rate_limit_events,
    ROUND(COUNT(*) / 60.0, 3) AS rate_limits_per_second
FROM genai_logs
WHERE event = 'llm_rate_limited'
  AND timestamp >= '2025-01-01T00:00:00'
GROUP BY 1, 2
ORDER BY 1 DESC
LIMIT 60;


-- ─────────────────────────────────────────────────────────────────
-- 4. TOKEN USAGE ANALYSIS — Cost attribution
-- ─────────────────────────────────────────────────────────────────
SELECT
    date(from_iso8601_timestamp(timestamp)) AS log_date,
    model,
    SUM(input_tokens) AS total_input_tokens,
    SUM(output_tokens) AS total_output_tokens,
    SUM(input_tokens + output_tokens) AS total_tokens,
    -- Approximate cost (Groq pricing as of 2025)
    ROUND(SUM(input_tokens) / 1e6 * 0.05
          + SUM(output_tokens) / 1e6 * 0.08, 4) AS estimated_cost_usd,
    COUNT(*) AS request_count,
    ROUND(AVG(output_tokens * 1.0 / NULLIF(input_tokens, 0)), 2) AS output_input_ratio
FROM genai_logs
WHERE event = 'llm_inference_success'
  AND timestamp >= '2025-01-01'
GROUP BY 1, 2
ORDER BY 1 DESC, estimated_cost_usd DESC;


-- ─────────────────────────────────────────────────────────────────
-- 5. AGENT LOOP DETECTION
-- "Which sessions were stuck in infinite loops?"
-- ─────────────────────────────────────────────────────────────────
SELECT
    session_id,
    agent_name,
    MAX(steps_taken) AS steps_taken,
    MIN(timestamp) AS session_start,
    MAX(timestamp) AS session_end,
    MAX(duration_ms) AS total_duration_ms,
    status,
    -- List all tool calls in order
    ARRAY_JOIN(ARRAY_AGG(tool_name ORDER BY step ASC), ' → ') AS tool_call_sequence
FROM agent_logs
WHERE event IN ('agent_session_completed', 'agent_tool_call')
  AND timestamp >= '2025-01-01T00:00:00'
GROUP BY session_id, agent_name, status
HAVING MAX(steps_taken) >= 8   -- Sessions that nearly or fully hit max_steps
ORDER BY steps_taken DESC, total_duration_ms DESC
LIMIT 50;


-- ─────────────────────────────────────────────────────────────────
-- 6. ML FRAUD MODEL — Low confidence predictions
-- "Is the model becoming uncertain about a population of transactions?"
-- ─────────────────────────────────────────────────────────────────
SELECT
    date_trunc('hour', from_iso8601_timestamp(timestamp)) AS hour,
    model_version,
    COUNT(*) AS total_predictions,
    SUM(CASE WHEN confidence < 0.6 THEN 1 ELSE 0 END) AS low_confidence_count,
    ROUND(100.0 * SUM(CASE WHEN confidence < 0.6 THEN 1 ELSE 0 END) / COUNT(*), 2) AS low_conf_pct,
    ROUND(AVG(confidence), 4) AS avg_confidence,
    ROUND(AVG(fraud_probability), 4) AS avg_fraud_prob,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_count
FROM ml_logs
WHERE event = 'ml_inference_success'
  AND timestamp >= '2025-01-01T00:00:00'
GROUP BY 1, 2
ORDER BY 1 DESC;


-- ─────────────────────────────────────────────────────────────────
-- 7. LATENCY PERCENTILES — P50, P90, P99, P999
-- "What were the latency tail numbers during the incident?"
-- ─────────────────────────────────────────────────────────────────
SELECT
    service,
    model,
    COUNT(*) AS requests,
    ROUND(APPROX_PERCENTILE(latency_ms, 0.50), 1) AS p50_ms,
    ROUND(APPROX_PERCENTILE(latency_ms, 0.90), 1) AS p90_ms,
    ROUND(APPROX_PERCENTILE(latency_ms, 0.99), 1) AS p99_ms,
    ROUND(APPROX_PERCENTILE(latency_ms, 0.999), 1) AS p999_ms,
    ROUND(MAX(latency_ms), 1) AS max_ms,
    ROUND(AVG(latency_ms), 1) AS avg_ms
FROM genai_logs
WHERE event = 'llm_inference_success'
  AND timestamp >= '2025-01-01T00:00:00'
  AND timestamp <= '2025-01-02T00:00:00'
GROUP BY service, model
ORDER BY p99_ms DESC;


-- ─────────────────────────────────────────────────────────────────
-- 8. KAFKA LAG TRENDING
-- "How fast was consumer lag growing during the incident?"
-- ─────────────────────────────────────────────────────────────────
SELECT
    date_trunc('minute', from_iso8601_timestamp(timestamp)) AS minute,
    consumer_group,
    topic,
    partition,
    MAX(lag) AS max_lag,
    AVG(lag) AS avg_lag,
    MAX(lag) - MIN(lag) AS lag_delta   -- growth per minute
FROM kafka_consumer_offsets
WHERE timestamp >= '2025-01-01T00:00:00'
  AND timestamp <= '2025-01-02T00:00:00'
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, max_lag DESC;


-- ─────────────────────────────────────────────────────────────────
-- 9. CONTEXT WINDOW SATURATION
-- "Were we hitting context limits silently?"
-- ─────────────────────────────────────────────────────────────────
SELECT
    model,
    COUNT(*) AS requests,
    SUM(CASE WHEN ctx_utilization > 0.85 THEN 1 ELSE 0 END) AS near_limit_count,
    ROUND(AVG(ctx_utilization), 3) AS avg_ctx_utilization,
    ROUND(MAX(ctx_utilization), 3) AS max_ctx_utilization,
    ROUND(
        100.0 * SUM(CASE WHEN ctx_utilization > 0.85 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS pct_near_limit,
    ROUND(AVG(input_tokens + output_tokens), 0) AS avg_total_tokens
FROM genai_logs
WHERE event = 'llm_inference_success'
  AND timestamp >= '2025-01-01T00:00:00'
GROUP BY model
ORDER BY pct_near_limit DESC;


-- ─────────────────────────────────────────────────────────────────
-- 10. INCIDENT TIMELINE — Full chronological event view
-- "What happened and in what order?"
-- ─────────────────────────────────────────────────────────────────
SELECT
    timestamp,
    service,
    level,
    event,
    COALESCE(request_id, session_id, transaction_id) AS entity_id,
    COALESCE(error, error_type) AS error_info,
    latency_ms,
    model
FROM (
    SELECT timestamp, service, level, event, request_id AS request_id,
           NULL AS session_id, NULL AS transaction_id,
           error, error_type, latency_ms, model
    FROM genai_logs
    WHERE level IN ('ERROR', 'WARNING', 'CRITICAL')

    UNION ALL

    SELECT timestamp, service, level, event, NULL,
           session_id, NULL, error, NULL, duration_ms, NULL
    FROM agent_logs
    WHERE level IN ('ERROR', 'WARNING', 'CRITICAL')

    UNION ALL

    SELECT timestamp, service, level, event, NULL,
           NULL, transaction_id, error, NULL, latency_ms, NULL
    FROM ml_logs
    WHERE level IN ('ERROR', 'WARNING', 'CRITICAL')
)
WHERE timestamp >= '2025-01-01T00:00:00'
  AND timestamp <= '2025-01-02T00:00:00'
ORDER BY timestamp ASC
LIMIT 500;

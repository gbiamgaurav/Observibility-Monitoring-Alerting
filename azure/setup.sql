-- ═══════════════════════════════════════════════════════════════
-- Azure Synapse Serverless SQL — External Tables over ADLS Gen2
-- ═══════════════════════════════════════════════════════════════
-- Run once to register your ADLS Gen2 log data as queryable views.
-- Replace 'YOUR_STORAGE_ACCOUNT' and 'YOUR_CONTAINER' with real values.
-- Prerequisites:
--   • Service principal or managed identity with Storage Blob Data Reader
--   • Synapse workspace with a Serverless SQL pool
--   • Run in the built-in Serverless SQL endpoint
-- ─────────────────────────────────────────────────────────────────

-- 1. Create dedicated database
CREATE DATABASE pipeline_observability;
GO

USE pipeline_observability;
GO

-- ─────────────────────────────────────────────────────────────────
-- Credential + Data Source
-- ─────────────────────────────────────────────────────────────────

-- Use managed identity (preferred) or service principal SAS token.
-- Managed identity: assign Storage Blob Data Reader to the Synapse workspace MSI.
CREATE DATABASE SCOPED CREDENTIAL AzureStorageCredential
    WITH IDENTITY = 'Managed Identity';
GO

CREATE EXTERNAL DATA SOURCE PipelineLogsSource
WITH (
    LOCATION   = 'https://YOUR_STORAGE_ACCOUNT.dfs.core.windows.net/YOUR_CONTAINER',
    CREDENTIAL = AzureStorageCredential
);
GO


-- ─────────────────────────────────────────────────────────────────
-- GenAI Pipeline Logs
-- Synapse Serverless SQL reads JSON via OPENROWSET + OPENJSON.
-- Expose as a VIEW so callers use it like a table.
-- ─────────────────────────────────────────────────────────────────
CREATE OR ALTER VIEW genai_logs AS
SELECT
    JSON_VALUE(doc, '$.timestamp')      AS timestamp,
    JSON_VALUE(doc, '$.service')        AS service,
    JSON_VALUE(doc, '$.level')          AS level,
    JSON_VALUE(doc, '$.event')          AS event,
    JSON_VALUE(doc, '$.request_id')     AS request_id,
    JSON_VALUE(doc, '$.model')          AS model,
    TRY_CAST(JSON_VALUE(doc, '$.input_tokens')    AS BIGINT)  AS input_tokens,
    TRY_CAST(JSON_VALUE(doc, '$.output_tokens')   AS BIGINT)  AS output_tokens,
    TRY_CAST(JSON_VALUE(doc, '$.latency_ms')      AS FLOAT)   AS latency_ms,
    JSON_VALUE(doc, '$.finish_reason')             AS finish_reason,
    TRY_CAST(JSON_VALUE(doc, '$.ctx_utilization') AS FLOAT)   AS ctx_utilization,
    JSON_VALUE(doc, '$.error_type')     AS error_type,
    JSON_VALUE(doc, '$.error')          AS error,
    JSON_VALUE(doc, '$.environment')    AS environment
FROM OPENROWSET(
    BULK            'pipeline-logs/genai/**',
    DATA_SOURCE     = 'PipelineLogsSource',
    FORMAT          = 'CSV',
    FIELDTERMINATOR = '0x0b',   -- treat each line as a single field
    FIELDQUOTE      = '0x0b',
    ROWTERMINATOR   = '0x0a'
) WITH (doc NVARCHAR(MAX)) AS rows;
GO


-- ─────────────────────────────────────────────────────────────────
-- Agentic Pipeline Logs
-- ─────────────────────────────────────────────────────────────────
CREATE OR ALTER VIEW agent_logs AS
SELECT
    JSON_VALUE(doc, '$.timestamp')   AS timestamp,
    JSON_VALUE(doc, '$.service')     AS service,
    JSON_VALUE(doc, '$.level')       AS level,
    JSON_VALUE(doc, '$.event')       AS event,
    JSON_VALUE(doc, '$.session_id')  AS session_id,
    JSON_VALUE(doc, '$.agent_name')  AS agent_name,
    TRY_CAST(JSON_VALUE(doc, '$.step')        AS INT)   AS step,
    JSON_VALUE(doc, '$.tool_name')             AS tool_name,
    TRY_CAST(JSON_VALUE(doc, '$.steps_taken') AS INT)   AS steps_taken,
    TRY_CAST(JSON_VALUE(doc, '$.duration_ms') AS FLOAT) AS duration_ms,
    JSON_VALUE(doc, '$.status')      AS status,
    JSON_VALUE(doc, '$.error')       AS error,
    JSON_VALUE(doc, '$.environment') AS environment
FROM OPENROWSET(
    BULK            'pipeline-logs/agentic/**',
    DATA_SOURCE     = 'PipelineLogsSource',
    FORMAT          = 'CSV',
    FIELDTERMINATOR = '0x0b',
    FIELDQUOTE      = '0x0b',
    ROWTERMINATOR   = '0x0a'
) WITH (doc NVARCHAR(MAX)) AS rows;
GO


-- ─────────────────────────────────────────────────────────────────
-- ML Pipeline Logs (Hive-partitioned: year=YYYY/month=MM/day=DD)
-- ─────────────────────────────────────────────────────────────────
CREATE OR ALTER VIEW ml_logs AS
SELECT
    JSON_VALUE(doc, '$.timestamp')          AS timestamp,
    JSON_VALUE(doc, '$.service')            AS service,
    JSON_VALUE(doc, '$.level')              AS level,
    JSON_VALUE(doc, '$.event')              AS event,
    JSON_VALUE(doc, '$.transaction_id')     AS transaction_id,
    TRY_CAST(JSON_VALUE(doc, '$.is_fraud')          AS BIT)   AS is_fraud,
    TRY_CAST(JSON_VALUE(doc, '$.fraud_probability') AS FLOAT) AS fraud_probability,
    TRY_CAST(JSON_VALUE(doc, '$.confidence')        AS FLOAT) AS confidence,
    TRY_CAST(JSON_VALUE(doc, '$.latency_ms')        AS FLOAT) AS latency_ms,
    JSON_VALUE(doc, '$.model_version')      AS model_version,
    JSON_VALUE(doc, '$.error')              AS error
FROM OPENROWSET(
    BULK            'pipeline-logs/ml/**',
    DATA_SOURCE     = 'PipelineLogsSource',
    FORMAT          = 'CSV',
    FIELDTERMINATOR = '0x0b',
    FIELDQUOTE      = '0x0b',
    ROWTERMINATOR   = '0x0a'
) WITH (doc NVARCHAR(MAX)) AS rows;
GO


-- ─────────────────────────────────────────────────────────────────
-- Kafka Consumer Offset Logs
-- ─────────────────────────────────────────────────────────────────
CREATE OR ALTER VIEW kafka_consumer_offsets AS
SELECT
    JSON_VALUE(doc, '$.timestamp')       AS timestamp,
    JSON_VALUE(doc, '$.consumer_group')  AS consumer_group,
    JSON_VALUE(doc, '$.topic')           AS topic,
    TRY_CAST(JSON_VALUE(doc, '$.partition')       AS INT)    AS partition,
    TRY_CAST(JSON_VALUE(doc, '$.current_offset')  AS BIGINT) AS current_offset,
    TRY_CAST(JSON_VALUE(doc, '$.log_end_offset')  AS BIGINT) AS log_end_offset,
    TRY_CAST(JSON_VALUE(doc, '$.lag')             AS BIGINT) AS lag
FROM OPENROWSET(
    BULK            'kafka-metrics/**',
    DATA_SOURCE     = 'PipelineLogsSource',
    FORMAT          = 'CSV',
    FIELDTERMINATOR = '0x0b',
    FIELDQUOTE      = '0x0b',
    ROWTERMINATOR   = '0x0a'
) WITH (doc NVARCHAR(MAX)) AS rows;
GO

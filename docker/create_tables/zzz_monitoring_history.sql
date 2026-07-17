-- Multi-resolution monitoring history.
--
-- Detailed source events remain short-lived. Aggregate tables retain one-minute
-- data for seven days, fifteen-minute data for thirty days, and hourly data for
-- ninety days. The history views use non-overlapping time windows so queries do
-- not double-count buckets available at more than one resolution.

CREATE TABLE IF NOT EXISTS alerts_15m (
    time_bucket DateTime NOT NULL,
    src_ip String NOT NULL,
    alert_count SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(time_bucket)
ORDER BY (time_bucket, src_ip)
TTL toDateTime(time_bucket) + INTERVAL 30 DAY;

ALTER TABLE alerts_15m
MODIFY TTL toDateTime(time_bucket) + INTERVAL 30 DAY;

INSERT INTO alerts_15m
SELECT
    toStartOfInterval(alert_timestamp, INTERVAL 15 MINUTE) AS time_bucket,
    src_ip,
    count() AS alert_count
FROM alerts
WHERE alert_timestamp >= now() - INTERVAL 30 DAY
  AND (SELECT count() FROM alerts_15m) = 0
GROUP BY time_bucket, src_ip;

CREATE MATERIALIZED VIEW IF NOT EXISTS alerts_15m_mv
TO alerts_15m
AS
SELECT
    toStartOfInterval(alert_timestamp, INTERVAL 15 MINUTE) AS time_bucket,
    src_ip,
    count() AS alert_count
FROM alerts
GROUP BY time_bucket, src_ip;

CREATE TABLE IF NOT EXISTS alerts_1h (
    time_bucket DateTime NOT NULL,
    src_ip String NOT NULL,
    alert_count SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(time_bucket)
ORDER BY (time_bucket, src_ip)
TTL toDateTime(time_bucket) + INTERVAL 90 DAY;

ALTER TABLE alerts_1h
MODIFY TTL toDateTime(time_bucket) + INTERVAL 90 DAY;

INSERT INTO alerts_1h
SELECT
    toStartOfHour(alert_timestamp) AS time_bucket,
    src_ip,
    count() AS alert_count
FROM alerts
WHERE alert_timestamp >= now() - INTERVAL 90 DAY
  AND (SELECT count() FROM alerts_1h) = 0
GROUP BY time_bucket, src_ip;

CREATE MATERIALIZED VIEW IF NOT EXISTS alerts_1h_mv
TO alerts_1h
AS
SELECT
    toStartOfHour(alert_timestamp) AS time_bucket,
    src_ip,
    count() AS alert_count
FROM alerts
GROUP BY time_bucket, src_ip;

CREATE OR REPLACE VIEW alerts_history AS
SELECT time_bucket, src_ip, alert_count
FROM alerts_1m
WHERE time_bucket >= now() - INTERVAL 7 DAY
UNION ALL
SELECT time_bucket, src_ip, alert_count
FROM alerts_15m
WHERE time_bucket >= now() - INTERVAL 30 DAY
  AND time_bucket < now() - INTERVAL 7 DAY
UNION ALL
SELECT time_bucket, src_ip, alert_count
FROM alerts_1h
WHERE time_bucket >= now() - INTERVAL 90 DAY
  AND time_bucket < now() - INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS fill_levels_15m (
    time_bucket DateTime NOT NULL,
    stage LowCardinality(String) NOT NULL,
    entry_type LowCardinality(String) NOT NULL,
    min_count SimpleAggregateFunction(min, UInt32),
    avg_count AggregateFunction(avg, UInt32),
    max_count SimpleAggregateFunction(max, UInt32),
    median_count AggregateFunction(quantileTDigest(0.5), UInt32)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(time_bucket)
ORDER BY (stage, entry_type, time_bucket)
TTL toDateTime(time_bucket) + INTERVAL 30 DAY;

ALTER TABLE fill_levels_15m
MODIFY TTL toDateTime(time_bucket) + INTERVAL 30 DAY;

INSERT INTO fill_levels_15m
SELECT
    toStartOfInterval(timestamp, INTERVAL 15 MINUTE) AS time_bucket,
    stage,
    entry_type,
    min(entry_count) AS min_count,
    avgState(entry_count) AS avg_count,
    max(entry_count) AS max_count,
    quantileTDigestState(0.5)(entry_count) AS median_count
FROM fill_levels
WHERE timestamp >= now() - INTERVAL 30 DAY
  AND (SELECT count() FROM fill_levels_15m) = 0
GROUP BY time_bucket, stage, entry_type;

CREATE MATERIALIZED VIEW IF NOT EXISTS fill_levels_15m_mv
TO fill_levels_15m
AS
SELECT
    toStartOfInterval(timestamp, INTERVAL 15 MINUTE) AS time_bucket,
    stage,
    entry_type,
    min(entry_count) AS min_count,
    avgState(entry_count) AS avg_count,
    max(entry_count) AS max_count,
    quantileTDigestState(0.5)(entry_count) AS median_count
FROM fill_levels
GROUP BY time_bucket, stage, entry_type;

CREATE TABLE IF NOT EXISTS fill_levels_1h (
    time_bucket DateTime NOT NULL,
    stage LowCardinality(String) NOT NULL,
    entry_type LowCardinality(String) NOT NULL,
    min_count SimpleAggregateFunction(min, UInt32),
    avg_count AggregateFunction(avg, UInt32),
    max_count SimpleAggregateFunction(max, UInt32),
    median_count AggregateFunction(quantileTDigest(0.5), UInt32)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(time_bucket)
ORDER BY (stage, entry_type, time_bucket)
TTL toDateTime(time_bucket) + INTERVAL 90 DAY;

ALTER TABLE fill_levels_1h
MODIFY TTL toDateTime(time_bucket) + INTERVAL 90 DAY;

INSERT INTO fill_levels_1h
SELECT
    toStartOfHour(timestamp) AS time_bucket,
    stage,
    entry_type,
    min(entry_count) AS min_count,
    avgState(entry_count) AS avg_count,
    max(entry_count) AS max_count,
    quantileTDigestState(0.5)(entry_count) AS median_count
FROM fill_levels
WHERE timestamp >= now() - INTERVAL 90 DAY
  AND (SELECT count() FROM fill_levels_1h) = 0
GROUP BY time_bucket, stage, entry_type;

CREATE MATERIALIZED VIEW IF NOT EXISTS fill_levels_1h_mv
TO fill_levels_1h
AS
SELECT
    toStartOfHour(timestamp) AS time_bucket,
    stage,
    entry_type,
    min(entry_count) AS min_count,
    avgState(entry_count) AS avg_count,
    max(entry_count) AS max_count,
    quantileTDigestState(0.5)(entry_count) AS median_count
FROM fill_levels
GROUP BY time_bucket, stage, entry_type;

CREATE OR REPLACE VIEW fill_levels_history AS
SELECT
    time_bucket,
    stage,
    entry_type,
    min_count,
    avg_count,
    max_count,
    median_count
FROM fill_levels_1m
WHERE time_bucket >= now() - INTERVAL 7 DAY
UNION ALL
SELECT
    time_bucket,
    stage,
    entry_type,
    min_count,
    avg_count,
    max_count,
    median_count
FROM fill_levels_15m
WHERE time_bucket >= now() - INTERVAL 30 DAY
  AND time_bucket < now() - INTERVAL 7 DAY
UNION ALL
SELECT
    time_bucket,
    stage,
    entry_type,
    min_count,
    avg_count,
    max_count,
    median_count
FROM fill_levels_1h
WHERE time_bucket >= now() - INTERVAL 90 DAY
  AND time_bucket < now() - INTERVAL 30 DAY;

-- Latency values are first completed per source entity. This avoids calculating
-- a duration from start/end events that arrived in different insert blocks.

CREATE OR REPLACE VIEW server_log_current_latency_values AS
SELECT
    sl.message_id AS message_id,
    toDate(slt.event_timestamp) AS event_date,
    sl.timestamp_in AS start_timestamp,
    slt.event_timestamp AS end_timestamp,
    dateDiff('microsecond', sl.timestamp_in, slt.event_timestamp) AS latency_us
FROM server_logs sl
INNER JOIN server_logs_timestamps slt ON sl.message_id = slt.message_id
WHERE slt.event = 'timestamp_out'
  AND slt.event_timestamp > sl.timestamp_in;

CREATE OR REPLACE VIEW logline_stage_current_latency_values AS
SELECT
    stage,
    logline_id,
    min(event_date) AS event_date,
    minMerge(start_timestamp) AS start_timestamp,
    maxMerge(end_timestamp) AS end_timestamp,
    dateDiff('microsecond', start_timestamp, end_timestamp) AS latency_us
FROM logline_stage_latencies
GROUP BY stage, logline_id
HAVING start_timestamp > toDateTime64(0, 6)
   AND end_timestamp > start_timestamp;

CREATE OR REPLACE VIEW batch_stage_current_latency_values AS
SELECT
    stage,
    instance_name,
    batch_id,
    min(event_date) AS event_date,
    minMerge(start_timestamp) AS start_timestamp,
    maxMerge(end_timestamp) AS end_timestamp,
    dateDiff('microsecond', start_timestamp, end_timestamp) AS latency_us
FROM batch_stage_latencies
GROUP BY stage, instance_name, batch_id
HAVING start_timestamp > toDateTime64(0, 6)
   AND end_timestamp > start_timestamp;

CREATE OR REPLACE VIEW suspicious_batch_stage_current_latency_values AS
SELECT
    stage,
    instance_name,
    suspicious_batch_id,
    min(event_date) AS event_date,
    minMerge(start_timestamp) AS start_timestamp,
    maxMerge(end_timestamp) AS end_timestamp,
    dateDiff('microsecond', start_timestamp, end_timestamp) AS latency_us
FROM suspicious_batch_stage_latencies
GROUP BY stage, instance_name, suspicious_batch_id
HAVING start_timestamp > toDateTime64(0, 6)
   AND end_timestamp > start_timestamp;

CREATE OR REPLACE VIEW pipeline_roundtrip_current_latency_values AS
SELECT
    message_id,
    toDate(terminal_timestamp) AS event_date,
    start_timestamp,
    terminal_timestamp AS end_timestamp,
    dateDiff('microsecond', start_timestamp, terminal_timestamp) AS latency_us
FROM (
    SELECT
        sl.message_id AS message_id,
        min(sl.timestamp_in) AS start_timestamp,
        max(terminal.terminal_timestamp) AS terminal_timestamp
    FROM server_logs sl
    INNER JOIN (
        SELECT
            sltl.message_id AS message_id,
            max(lt.timestamp) AS terminal_timestamp
        FROM server_log_to_logline sltl
        INNER JOIN logline_timestamps lt ON sltl.logline_id = lt.logline_id
        WHERE lt.is_active = false
        GROUP BY sltl.message_id
        UNION ALL
        SELECT
            message_id,
            max(timestamp) AS terminal_timestamp
        FROM server_log_terminal_events
        GROUP BY message_id
    ) AS terminal ON sl.message_id = terminal.message_id
    GROUP BY sl.message_id
)
WHERE terminal_timestamp > start_timestamp;

CREATE OR REPLACE VIEW pipeline_latency_current_values AS
SELECT
    'server_log' AS family,
    'log_storage.logserver' AS stage,
    end_timestamp,
    toFloat64(latency_us) AS latency_us
FROM server_log_current_latency_values
UNION ALL
SELECT
    'logline' AS family,
    stage,
    end_timestamp,
    toFloat64(latency_us) AS latency_us
FROM logline_stage_current_latency_values
UNION ALL
SELECT
    'batch' AS family,
    stage,
    end_timestamp,
    toFloat64(latency_us) AS latency_us
FROM batch_stage_current_latency_values
UNION ALL
SELECT
    'suspicious_batch' AS family,
    stage,
    end_timestamp,
    toFloat64(latency_us) AS latency_us
FROM suspicious_batch_stage_current_latency_values
UNION ALL
SELECT
    'roundtrip' AS family,
    'pipeline.roundtrip' AS stage,
    end_timestamp,
    toFloat64(latency_us) AS latency_us
FROM pipeline_roundtrip_current_latency_values
UNION ALL
SELECT
    'transport' AS family,
    'transport.batch_handler_to_prefilter' AS stage,
    bt2.timestamp AS end_timestamp,
    toFloat64(dateDiff('microsecond', bt1.timestamp, bt2.timestamp)) AS latency_us
FROM batch_tree bt1
INNER JOIN batch_tree bt2 ON bt1.batch_row_id = bt2.parent_batch_row_id
WHERE bt1.stage = 'log_collection.batch_handler'
  AND bt1.status = 'completed'
  AND bt2.stage = 'log_filtering.prefilter'
  AND bt2.status = 'in_process'
  AND bt2.timestamp > bt1.timestamp
UNION ALL
SELECT
    'transport' AS family,
    'transport.prefilter_to_inspector' AS stage,
    bt2.timestamp AS end_timestamp,
    toFloat64(dateDiff('microsecond', bt1.timestamp, bt2.timestamp)) AS latency_us
FROM batch_tree bt1
INNER JOIN batch_tree bt2 ON bt1.batch_row_id = bt2.parent_batch_row_id
WHERE bt1.stage = 'log_filtering.prefilter'
  AND bt1.status = 'finished'
  AND bt2.stage = 'data_inspection.inspector'
  AND bt2.status = 'in_process'
  AND bt2.timestamp > bt1.timestamp
UNION ALL
SELECT
    'transport' AS family,
    'transport.inspector_to_detector' AS stage,
    bt2.timestamp AS end_timestamp,
    toFloat64(dateDiff('microsecond', bt1.timestamp, bt2.timestamp)) AS latency_us
FROM batch_tree bt1
INNER JOIN batch_tree bt2 ON bt1.batch_row_id = bt2.parent_batch_row_id
WHERE bt1.stage = 'data_inspection.inspector'
  AND bt1.status = 'finished'
  AND bt2.stage = 'data_analysis.detector'
  AND bt2.status = 'in_process'
  AND bt2.timestamp > bt1.timestamp;

CREATE TABLE IF NOT EXISTS pipeline_latency_1m (
    time_bucket DateTime NOT NULL,
    family LowCardinality(String) NOT NULL,
    stage LowCardinality(String) NOT NULL,
    sample_count UInt64 NOT NULL,
    min_latency_us Float64 NOT NULL,
    avg_latency_us Float64 NOT NULL,
    p50_latency_us Float64 NOT NULL,
    p95_latency_us Float64 NOT NULL,
    p99_latency_us Float64 NOT NULL,
    max_latency_us Float64 NOT NULL,
    snapshot_version DateTime64(3) NOT NULL
)
ENGINE = ReplacingMergeTree(snapshot_version)
PARTITION BY toYYYYMM(time_bucket)
ORDER BY (family, stage, time_bucket)
TTL toDateTime(time_bucket) + INTERVAL 7 DAY;

ALTER TABLE pipeline_latency_1m
MODIFY TTL toDateTime(time_bucket) + INTERVAL 7 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS pipeline_latency_1m_refresh
REFRESH EVERY 1 MINUTE APPEND TO pipeline_latency_1m
AS
SELECT
    toStartOfMinute(end_timestamp) AS time_bucket,
    family,
    stage,
    count() AS sample_count,
    min(latency_us) AS min_latency_us,
    avg(latency_us) AS avg_latency_us,
    quantileTDigest(0.5)(latency_us) AS p50_latency_us,
    quantileTDigest(0.95)(latency_us) AS p95_latency_us,
    quantileTDigest(0.99)(latency_us) AS p99_latency_us,
    max(latency_us) AS max_latency_us,
    now64(3) AS snapshot_version
FROM pipeline_latency_current_values
WHERE end_timestamp >= now() - INTERVAL 2 HOUR
  AND end_timestamp < toStartOfMinute(now())
GROUP BY time_bucket, family, stage;

CREATE TABLE IF NOT EXISTS pipeline_latency_15m (
    time_bucket DateTime NOT NULL,
    family LowCardinality(String) NOT NULL,
    stage LowCardinality(String) NOT NULL,
    sample_count UInt64 NOT NULL,
    min_latency_us Float64 NOT NULL,
    avg_latency_us Float64 NOT NULL,
    p50_latency_us Float64 NOT NULL,
    p95_latency_us Float64 NOT NULL,
    p99_latency_us Float64 NOT NULL,
    max_latency_us Float64 NOT NULL,
    snapshot_version DateTime64(3) NOT NULL
)
ENGINE = ReplacingMergeTree(snapshot_version)
PARTITION BY toYYYYMM(time_bucket)
ORDER BY (family, stage, time_bucket)
TTL toDateTime(time_bucket) + INTERVAL 30 DAY;

ALTER TABLE pipeline_latency_15m
MODIFY TTL toDateTime(time_bucket) + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS pipeline_latency_15m_refresh
REFRESH EVERY 5 MINUTE APPEND TO pipeline_latency_15m
AS
SELECT
    toStartOfInterval(end_timestamp, INTERVAL 15 MINUTE) AS time_bucket,
    family,
    stage,
    count() AS sample_count,
    min(latency_us) AS min_latency_us,
    avg(latency_us) AS avg_latency_us,
    quantileTDigest(0.5)(latency_us) AS p50_latency_us,
    quantileTDigest(0.95)(latency_us) AS p95_latency_us,
    quantileTDigest(0.99)(latency_us) AS p99_latency_us,
    max(latency_us) AS max_latency_us,
    now64(3) AS snapshot_version
FROM pipeline_latency_current_values
WHERE end_timestamp >= now() - INTERVAL 6 HOUR
  AND end_timestamp < toStartOfInterval(now(), INTERVAL 15 MINUTE)
GROUP BY time_bucket, family, stage;

CREATE TABLE IF NOT EXISTS pipeline_latency_1h (
    time_bucket DateTime NOT NULL,
    family LowCardinality(String) NOT NULL,
    stage LowCardinality(String) NOT NULL,
    sample_count UInt64 NOT NULL,
    min_latency_us Float64 NOT NULL,
    avg_latency_us Float64 NOT NULL,
    p50_latency_us Float64 NOT NULL,
    p95_latency_us Float64 NOT NULL,
    p99_latency_us Float64 NOT NULL,
    max_latency_us Float64 NOT NULL,
    snapshot_version DateTime64(3) NOT NULL
)
ENGINE = ReplacingMergeTree(snapshot_version)
PARTITION BY toYYYYMM(time_bucket)
ORDER BY (family, stage, time_bucket)
TTL toDateTime(time_bucket) + INTERVAL 90 DAY;

ALTER TABLE pipeline_latency_1h
MODIFY TTL toDateTime(time_bucket) + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS pipeline_latency_1h_refresh
REFRESH EVERY 15 MINUTE APPEND TO pipeline_latency_1h
AS
SELECT
    toStartOfHour(end_timestamp) AS time_bucket,
    family,
    stage,
    count() AS sample_count,
    min(latency_us) AS min_latency_us,
    avg(latency_us) AS avg_latency_us,
    quantileTDigest(0.5)(latency_us) AS p50_latency_us,
    quantileTDigest(0.95)(latency_us) AS p95_latency_us,
    quantileTDigest(0.99)(latency_us) AS p99_latency_us,
    max(latency_us) AS max_latency_us,
    now64(3) AS snapshot_version
FROM pipeline_latency_current_values
WHERE end_timestamp >= now() - INTERVAL 12 HOUR
  AND end_timestamp < toStartOfHour(now())
GROUP BY time_bucket, family, stage;

CREATE OR REPLACE VIEW pipeline_latency_rollup_history AS
SELECT * EXCEPT snapshot_version
FROM pipeline_latency_1m FINAL
WHERE time_bucket >= now() - INTERVAL 7 DAY
  AND time_bucket < now() - INTERVAL 1 DAY
UNION ALL
SELECT * EXCEPT snapshot_version
FROM pipeline_latency_15m FINAL
WHERE time_bucket >= now() - INTERVAL 30 DAY
  AND time_bucket < now() - INTERVAL 7 DAY
UNION ALL
SELECT * EXCEPT snapshot_version
FROM pipeline_latency_1h FINAL
WHERE time_bucket >= now() - INTERVAL 90 DAY
  AND time_bucket < now() - INTERVAL 30 DAY;

CREATE OR REPLACE VIEW pipeline_latency_values AS
SELECT family, stage, end_timestamp, latency_us
FROM pipeline_latency_current_values
WHERE end_timestamp >= now() - INTERVAL 1 DAY
UNION ALL
SELECT
    family,
    stage,
    toDateTime64(time_bucket, 6) AS end_timestamp,
    p50_latency_us AS latency_us
FROM pipeline_latency_rollup_history;

-- Keep the existing dashboard-facing schemas. Recent ranges contain exact
-- per-entity durations; older ranges contain one representative p50 row per
-- retained time bucket.

CREATE OR REPLACE VIEW server_log_latency_values AS
SELECT
    message_id,
    event_date,
    start_timestamp,
    end_timestamp,
    toFloat64(latency_us) AS latency_us
FROM server_log_current_latency_values
WHERE end_timestamp >= now() - INTERVAL 1 DAY
UNION ALL
SELECT
    toUUID('00000000-0000-0000-0000-000000000000') AS message_id,
    toDate(time_bucket) AS event_date,
    toDateTime64(time_bucket, 6) AS start_timestamp,
    toDateTime64(time_bucket, 6) AS end_timestamp,
    p50_latency_us AS latency_us
FROM pipeline_latency_rollup_history
WHERE family = 'server_log';

CREATE OR REPLACE VIEW logline_stage_latency_values AS
SELECT
    stage,
    logline_id,
    event_date,
    start_timestamp,
    end_timestamp,
    toFloat64(latency_us) AS latency_us
FROM logline_stage_current_latency_values
WHERE end_timestamp >= now() - INTERVAL 1 DAY
UNION ALL
SELECT
    stage,
    toUUID('00000000-0000-0000-0000-000000000000') AS logline_id,
    toDate(time_bucket) AS event_date,
    toDateTime64(time_bucket, 6) AS start_timestamp,
    toDateTime64(time_bucket, 6) AS end_timestamp,
    p50_latency_us AS latency_us
FROM pipeline_latency_rollup_history
WHERE family = 'logline';

CREATE OR REPLACE VIEW batch_stage_latency_values AS
SELECT
    stage,
    instance_name,
    batch_id,
    event_date,
    start_timestamp,
    end_timestamp,
    toFloat64(latency_us) AS latency_us
FROM batch_stage_current_latency_values
WHERE end_timestamp >= now() - INTERVAL 1 DAY
UNION ALL
SELECT
    stage,
    'historical_rollup' AS instance_name,
    toUUID('00000000-0000-0000-0000-000000000000') AS batch_id,
    toDate(time_bucket) AS event_date,
    toDateTime64(time_bucket, 6) AS start_timestamp,
    toDateTime64(time_bucket, 6) AS end_timestamp,
    p50_latency_us AS latency_us
FROM pipeline_latency_rollup_history
WHERE family = 'batch';

CREATE OR REPLACE VIEW suspicious_batch_stage_latency_values AS
SELECT
    stage,
    instance_name,
    suspicious_batch_id,
    event_date,
    start_timestamp,
    end_timestamp,
    toFloat64(latency_us) AS latency_us
FROM suspicious_batch_stage_current_latency_values
WHERE end_timestamp >= now() - INTERVAL 1 DAY
UNION ALL
SELECT
    stage,
    'historical_rollup' AS instance_name,
    toUUID('00000000-0000-0000-0000-000000000000') AS suspicious_batch_id,
    toDate(time_bucket) AS event_date,
    toDateTime64(time_bucket, 6) AS start_timestamp,
    toDateTime64(time_bucket, 6) AS end_timestamp,
    p50_latency_us AS latency_us
FROM pipeline_latency_rollup_history
WHERE family = 'suspicious_batch';

CREATE OR REPLACE VIEW pipeline_transport_latency_values AS
SELECT stage, end_timestamp, latency_us
FROM pipeline_latency_current_values
WHERE family = 'transport'
  AND end_timestamp >= now() - INTERVAL 1 DAY
UNION ALL
SELECT
    stage,
    toDateTime64(time_bucket, 6) AS end_timestamp,
    p50_latency_us AS latency_us
FROM pipeline_latency_rollup_history
WHERE family = 'transport';

CREATE OR REPLACE VIEW pipeline_roundtrip_latency_values AS
SELECT
    message_id,
    event_date,
    start_timestamp,
    end_timestamp,
    toFloat64(latency_us) AS latency_us
FROM pipeline_roundtrip_current_latency_values
WHERE end_timestamp >= now() - INTERVAL 1 DAY
UNION ALL
SELECT
    toUUID('00000000-0000-0000-0000-000000000000') AS message_id,
    toDate(time_bucket) AS event_date,
    toDateTime64(time_bucket, 6) AS start_timestamp,
    toDateTime64(time_bucket, 6) AS end_timestamp,
    p50_latency_us AS latency_us
FROM pipeline_latency_rollup_history
WHERE family = 'roundtrip';

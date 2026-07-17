CREATE TABLE IF NOT EXISTS alerts_1m (
    time_bucket DateTime NOT NULL,
    src_ip String NOT NULL,
    alert_count SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(time_bucket)
ORDER BY (time_bucket, src_ip)
TTL toDateTime(time_bucket) + INTERVAL 7 DAY;

ALTER TABLE alerts_1m
MODIFY TTL toDateTime(time_bucket) + INTERVAL 7 DAY;


CREATE MATERIALIZED VIEW IF NOT EXISTS alerts_1m_mv
TO alerts_1m
AS
SELECT
    toStartOfMinute(alert_timestamp) AS time_bucket,
    src_ip,
    count() AS alert_count
FROM alerts
GROUP BY time_bucket, src_ip;

CREATE TABLE IF NOT EXISTS fill_levels_1m (
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
TTL toDateTime(time_bucket) + INTERVAL 7 DAY;

ALTER TABLE fill_levels_1m
MODIFY TTL toDateTime(time_bucket) + INTERVAL 7 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS fill_levels_1m_mv
TO fill_levels_1m
AS
SELECT
    toStartOfMinute(timestamp) AS time_bucket,
    stage,
    entry_type,
    min(entry_count) AS min_count,
    avgState(entry_count) AS avg_count,
    max(entry_count) AS max_count,
    quantileTDigestState(0.5)(entry_count) AS median_count
FROM fill_levels
GROUP BY time_bucket, stage, entry_type;

CREATE TABLE IF NOT EXISTS server_log_latencies (
    event_date Date NOT NULL,
    message_id UUID NOT NULL,
    start_timestamp AggregateFunction(min, DateTime64(6)),
    end_timestamp AggregateFunction(max, DateTime64(6))
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, message_id)
TTL toDateTime(event_date) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS server_log_start_latency_mv
TO server_log_latencies
AS
SELECT
    toDate(timestamp_in) AS event_date,
    message_id,
    minState(timestamp_in) AS start_timestamp,
    maxState(toDateTime64(0, 6)) AS end_timestamp
FROM server_logs
GROUP BY event_date, message_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS server_log_end_latency_mv
TO server_log_latencies
AS
SELECT
    toDate(event_timestamp) AS event_date,
    message_id,
    minState(toDateTime64(0, 6)) AS start_timestamp,
    maxStateIf(event_timestamp, event = 'timestamp_out') AS end_timestamp
FROM server_logs_timestamps
GROUP BY event_date, message_id;

CREATE TABLE IF NOT EXISTS logline_stage_latencies (
    event_date Date NOT NULL,
    stage LowCardinality(String) NOT NULL,
    logline_id UUID NOT NULL,
    start_timestamp AggregateFunction(min, DateTime64(6)),
    end_timestamp AggregateFunction(max, DateTime64(6))
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (stage, event_date, logline_id)
TTL toDateTime(event_date) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS logline_stage_latencies_mv
TO logline_stage_latencies
AS
SELECT
    toDate(timestamp) AS event_date,
    stage,
    logline_id,
    minStateIf(timestamp, status = 'in_process') AS start_timestamp,
    maxStateIf(timestamp, status IN ('finished', 'batched', 'filtered_out', 'detected')) AS end_timestamp
FROM logline_timestamps
GROUP BY event_date, stage, logline_id;

CREATE TABLE IF NOT EXISTS batch_stage_latencies (
    event_date Date NOT NULL,
    stage LowCardinality(String) NOT NULL,
    instance_name LowCardinality(String) NOT NULL,
    batch_id UUID NOT NULL,
    start_timestamp AggregateFunction(min, DateTime64(6)),
    end_timestamp AggregateFunction(max, DateTime64(6))
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (stage, event_date, instance_name, batch_id)
TTL toDateTime(event_date) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS batch_stage_latencies_mv
TO batch_stage_latencies
AS
SELECT
    toDate(timestamp) AS event_date,
    stage,
    instance_name,
    batch_id,
    minStateIf(timestamp, status = 'in_process') AS start_timestamp,
    maxStateIf(timestamp, status IN ('finished', 'completed', 'batched', 'filtered_out') OR is_active = false) AS end_timestamp
FROM batch_timestamps
GROUP BY event_date, stage, instance_name, batch_id;

CREATE TABLE IF NOT EXISTS suspicious_batch_stage_latencies (
    event_date Date NOT NULL,
    stage LowCardinality(String) NOT NULL,
    instance_name LowCardinality(String) NOT NULL,
    suspicious_batch_id UUID NOT NULL,
    start_timestamp AggregateFunction(min, DateTime64(6)),
    end_timestamp AggregateFunction(max, DateTime64(6))
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (stage, event_date, instance_name, suspicious_batch_id)
TTL toDateTime(event_date) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS suspicious_batch_stage_latencies_mv
TO suspicious_batch_stage_latencies
AS
SELECT
    toDate(timestamp) AS event_date,
    stage,
    instance_name,
    suspicious_batch_id,
    minStateIf(timestamp, status = 'in_process') AS start_timestamp,
    maxStateIf(timestamp, status IN ('finished', 'completed', 'filtered_out', 'detected') OR is_active = false) AS end_timestamp
FROM suspicious_batch_timestamps
GROUP BY event_date, stage, instance_name, suspicious_batch_id;

CREATE VIEW IF NOT EXISTS server_log_latency_values AS
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

CREATE VIEW IF NOT EXISTS logline_stage_latency_values AS
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

CREATE VIEW IF NOT EXISTS batch_stage_latency_values AS
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

CREATE VIEW IF NOT EXISTS suspicious_batch_stage_latency_values AS
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

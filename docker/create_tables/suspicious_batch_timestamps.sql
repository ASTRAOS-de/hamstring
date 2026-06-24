CREATE TABLE IF NOT EXISTS suspicious_batch_timestamps (
    suspicious_batch_id UUID NOT NULL,
    src_ip String NOT NULL,
    instance_name LowCardinality(String) NOT NULL,
    stage LowCardinality(String) NOT NULL,
    status LowCardinality(String) NOT NULL,
    timestamp DateTime64(6) NOT NULL,
    message_count UInt32,
    is_active Bool NOT NULL
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (stage, status, timestamp, instance_name, suspicious_batch_id, src_ip)
TTL toDateTime(timestamp) + INTERVAL 1 DAY;
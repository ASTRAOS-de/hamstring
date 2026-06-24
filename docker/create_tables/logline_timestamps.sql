CREATE TABLE IF NOT EXISTS logline_timestamps (
    logline_id UUID NOT NULL,
    stage LowCardinality(String) NOT NULL,
    status LowCardinality(String) NOT NULL,
    timestamp DateTime64(6) NOT NULL,
    is_active Bool NOT NULL
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (stage, status, timestamp, logline_id)
TTL toDateTime(timestamp) + INTERVAL 1 DAY;

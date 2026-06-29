CREATE TABLE IF NOT EXISTS server_logs_timestamps (
    message_id UUID NOT NULL,
    event LowCardinality(String) NOT NULL,
    event_timestamp DateTime64(6) NOT NULL
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (event, event_timestamp, message_id)
TTL toDateTime(event_timestamp) + INTERVAL 1 DAY;
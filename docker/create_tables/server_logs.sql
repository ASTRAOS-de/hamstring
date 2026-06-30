CREATE TABLE IF NOT EXISTS server_logs (
    message_id UUID NOT NULL,
    timestamp_in DateTime64(6) NOT NULL,
    message_text String NOT NULL
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp_in)
ORDER BY (timestamp_in, message_id)
TTL toDateTime(timestamp_in) + INTERVAL 1 DAY;

CREATE TABLE IF NOT EXISTS server_log_to_logline (
    message_id UUID NOT NULL,
    logline_id UUID NOT NULL,
    timestamp DateTime64(6) NOT NULL,
)
ENGINE = MergeTree
ORDER BY (timestamp, message_id, logline_id)
PARTITION BY toYYYYMM(timestamp)
TTL toDateTime(timestamp) + INTERVAL 1 DAY;

CREATE TABLE IF NOT EXISTS server_log_terminal_events (
    message_id UUID NOT NULL,
    stage LowCardinality(String) NOT NULL,
    status LowCardinality(String) NOT NULL,
    timestamp DateTime64(6) NOT NULL
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (stage, status, timestamp, message_id);

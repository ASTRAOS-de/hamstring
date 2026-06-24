CREATE TABLE IF NOT EXISTS logline_to_batches (
    timestamp DateTime64(6) NOT NULL,
    logline_id UUID NOT NULL,
    batch_id UUID NOT NULL
)
ENGINE = MergeTree
ORDER BY (timestamp, batch_id, logline_id)
PARTITION BY toYYYYMM(timestamp)
TTL toDateTime(timestamp) + INTERVAL 1 DAY;



CREATE TABLE IF NOT EXISTS suspicious_batches_to_batch (
    timestamp DateTime64(6) NOT NULL,
    suspicious_batch_id UUID NOT NULL,
    batch_id UUID NOT NULL
)
ENGINE = MergeTree
ORDER BY (timestamp, batch_id, suspicious_batch_id)
PARTITION BY toYYYYMM(timestamp)
TTL toDateTime(timestamp) + INTERVAL 1 DAY;

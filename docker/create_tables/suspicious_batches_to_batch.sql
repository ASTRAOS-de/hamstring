CREATE TABLE IF NOT EXISTS suspicious_batches_to_batch (
    suspicious_batch_id UUID NOT NULL,
    batch_id UUID NOT NULL
)
ENGINE = MergeTree
ORDER BY (batch_id, suspicious_batch_id);

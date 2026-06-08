CREATE TABLE IF NOT EXISTS logline_to_batches (
    logline_id UUID NOT NULL,
    batch_id UUID NOT NULL
)
ENGINE = MergeTree
ORDER BY (batch_id, logline_id);

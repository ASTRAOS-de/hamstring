-- Table to be able to reconstruct where the batch was processed in
-- used in grafana to calculate the elapsed time between stages
CREATE TABLE IF NOT EXISTS batch_tree (
    batch_row_id String NOT NULL,
    batch_id UUID NOT NULL,
    parent_batch_row_id String DEFAULT '', -- Empty string indicates a root element
    instance_name LowCardinality(String) NOT NULL,
    stage LowCardinality(String) NOT NULL,
    status LowCardinality(String) NOT NULL,
    timestamp DateTime64(6) NOT NULL
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (stage, status, timestamp, instance_name, batch_row_id, parent_batch_row_id)
TTL toDateTime(timestamp) + INTERVAL 1 DAY;

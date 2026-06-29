CREATE TABLE IF NOT EXISTS fill_levels (
    timestamp DateTime64(6) NOT NULL,
    stage LowCardinality(String) NOT NULL,
    entry_type LowCardinality(String) NOT NULL,
    entry_count UInt32 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (stage, entry_type, timestamp)
TTL toDateTime(timestamp) + INTERVAL 1 DAY;

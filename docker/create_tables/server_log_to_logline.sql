CREATE TABLE IF NOT EXISTS server_log_to_logline (
    message_id UUID NOT NULL,
    logline_id UUID NOT NULL
)
ENGINE = MergeTree
ORDER BY (message_id, logline_id);

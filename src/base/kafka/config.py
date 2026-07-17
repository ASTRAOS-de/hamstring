"""Process-level Kafka configuration shared by all Kafka clients."""

import os

from src.base.retry import load_retry_settings
from src.base.utils import setup_config


HOSTNAME = os.getenv("HOSTNAME", "default_tid")
CONSUMER_GROUP_ID = os.getenv("GROUP_ID", "default_gid")
NUMBER_OF_INSTANCES = int(os.getenv("NUMBER_OF_INSTANCES", 1))

CONFIG = setup_config()
RETRY_SETTINGS = load_retry_settings(CONFIG)

KAFKA_BROKERS = CONFIG["environment"]["kafka_brokers"]
KAFKA_CONSUMER_CONFIG = CONFIG["environment"].get("kafka_consumer", {})
KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS = int(
    KAFKA_CONSUMER_CONFIG.get("max_poll_interval_ms", 1_800_000)
)

KAFKA_TRANSACTION_BATCH_CONFIG = CONFIG["environment"].get(
    "kafka_transaction_batch", {}
)
KAFKA_TRANSACTION_BATCH_SIZE = int(
    os.getenv(
        "KAFKA_TRANSACTION_BATCH_SIZE",
        KAFKA_TRANSACTION_BATCH_CONFIG.get("size", 100),
    )
)
KAFKA_TRANSACTION_BATCH_TIMEOUT_MS = int(
    os.getenv(
        "KAFKA_TRANSACTION_BATCH_TIMEOUT_MS",
        KAFKA_TRANSACTION_BATCH_CONFIG.get("timeout_ms", 50),
    )
)

KAFKA_TOPIC_CONFIG = CONFIG["environment"].get("kafka_topics", {})
KAFKA_TOPIC_DEFAULT_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", 12))
KAFKA_TOPIC_REPLICATION_FACTOR = int(
    os.getenv(
        "KAFKA_TOPIC_REPLICATION_FACTOR",
        KAFKA_TOPIC_CONFIG.get("replication_factor", len(KAFKA_BROKERS) or 1),
    )
)
KAFKA_TOPIC_AUTO_EXPAND_PARTITIONS = KAFKA_TOPIC_CONFIG.get(
    "auto_expand_partitions", True
)
KAFKA_TOPIC_STAGE_CONFIG = KAFKA_TOPIC_CONFIG.get("stages", {})
KAFKA_TOPIC_EXACT_CONFIG = KAFKA_TOPIC_CONFIG.get("topics", {})
KAFKA_PIPELINE_TOPIC_PREFIXES = (
    CONFIG["environment"].get("kafka_topics_prefix", {}).get("pipeline", {})
)


def bootstrap_servers() -> str:
    """Return the configured brokers in confluent-kafka format."""
    return ",".join(
        f"{broker['hostname']}:{broker['internal_port']}"
        for broker in KAFKA_BROKERS
    )

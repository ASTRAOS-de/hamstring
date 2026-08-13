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
KAFKA_PRODUCER_CONFIG = CONFIG["environment"].get("kafka_producer", {})
KAFKA_PRODUCER_COMPRESSION_TYPE = os.getenv(
    "KAFKA_PRODUCER_COMPRESSION_TYPE",
    KAFKA_PRODUCER_CONFIG.get("compression_type", "zstd"),
)

KAFKA_TRANSACTION_BATCH_CONFIG = CONFIG["environment"].get(
    "kafka_transaction_batch", {}
)
KAFKA_TRANSACTION_BATCH_STAGE_CONFIG = KAFKA_TRANSACTION_BATCH_CONFIG.get(
    "stages", {}
)
KAFKA_TRANSACTION_BATCH_TOPIC_CONFIG = KAFKA_TRANSACTION_BATCH_CONFIG.get(
    "topics", {}
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


def _transaction_batch_stage_config(stage: str | None) -> dict:
    """Resolve short and fully-qualified stage names, preferring the latter."""
    if not stage:
        return {}
    short_stage = str(stage).rsplit(".", 1)[-1]
    resolved = dict(KAFKA_TRANSACTION_BATCH_STAGE_CONFIG.get(short_stage, {}))
    resolved.update(KAFKA_TRANSACTION_BATCH_STAGE_CONFIG.get(str(stage), {}))
    return resolved


def transaction_batch_settings(
    topics: str | list[str], stage: str | None = None
) -> tuple[int, int]:
    """Resolve transaction size/timeout by topic, stage, then global defaults."""
    normalized_topics = [topics] if isinstance(topics, str) else list(topics)
    stage_config = _transaction_batch_stage_config(stage)
    global_config = {
        "size": KAFKA_TRANSACTION_BATCH_CONFIG.get("size", 100),
        "timeout_ms": KAFKA_TRANSACTION_BATCH_CONFIG.get("timeout_ms", 50),
    }

    effective_configs = []
    for topic in normalized_topics or [None]:
        effective = {**global_config, **stage_config}
        if topic is not None:
            effective.update(KAFKA_TRANSACTION_BATCH_TOPIC_CONFIG.get(topic, {}))
        effective_configs.append(effective)

    # A multi-topic consumer uses the most conservative effective value.
    size = min(max(1, int(item["size"])) for item in effective_configs)
    timeout_ms = min(max(0, int(item["timeout_ms"])) for item in effective_configs)

    # Deployment-level environment variables remain explicit final overrides.
    if "KAFKA_TRANSACTION_BATCH_SIZE" in os.environ:
        size = max(1, int(os.environ["KAFKA_TRANSACTION_BATCH_SIZE"]))
    if "KAFKA_TRANSACTION_BATCH_TIMEOUT_MS" in os.environ:
        timeout_ms = max(0, int(os.environ["KAFKA_TRANSACTION_BATCH_TIMEOUT_MS"]))
    return size, timeout_ms

KAFKA_TOPIC_CONFIG = CONFIG["environment"].get("kafka_topics", {})
KAFKA_TOPIC_DEFAULT_PARTITIONS = int(
    os.getenv("KAFKA_TOPIC_PARTITIONS", KAFKA_TOPIC_CONFIG.get("partitions", 12))
)
KAFKA_TOPIC_REPLICATION_FACTOR = int(
    os.getenv(
        "KAFKA_TOPIC_REPLICATION_FACTOR",
        KAFKA_TOPIC_CONFIG.get("replication_factor", len(KAFKA_BROKERS) or 1),
    )
)
KAFKA_TOPIC_AUTO_EXPAND_PARTITIONS = KAFKA_TOPIC_CONFIG.get(
    "auto_expand_partitions", True
)
KAFKA_TOPIC_DEFAULT_CONFIG = KAFKA_TOPIC_CONFIG.get("config", {})
KAFKA_TOPIC_STAGE_CONFIG = KAFKA_TOPIC_CONFIG.get("stages", {})
KAFKA_TOPIC_EXACT_CONFIG = KAFKA_TOPIC_CONFIG.get("topics", {})
KAFKA_PIPELINE_TOPIC_PREFIXES = (
    CONFIG["environment"].get("kafka_topics_prefix", {}).get("pipeline", {})
)


def bootstrap_servers() -> str:
    """Return the configured brokers in confluent-kafka format."""
    return ",".join(
        f"{broker['hostname']}:{broker['internal_port']}" for broker in KAFKA_BROKERS
    )

"""Process-level Kafka configuration shared by all client implementations."""

import os
from dataclasses import dataclass
from typing import Any

from src.base.retry import RetrySettings, load_retry_settings
from src.base.utils import setup_config


@dataclass(frozen=True)
class KafkaSettings:
    """Validated Kafka settings loaded once when a stage starts."""

    brokers: tuple[dict[str, Any], ...]
    consumer_max_poll_interval_ms: int
    max_record_bytes: int
    transaction_batch_size: int
    transaction_batch_timeout_ms: int
    transaction_commit_timeout_ms: int
    transaction_timeout_ms: int
    topic_default_partitions: int
    topic_replication_factor: int
    pipeline_mode: str
    topic_stage_config: dict[str, dict[str, Any]]
    topic_exact_config: dict[str, dict[str, Any]]
    pipeline_topic_prefixes: dict[str, str]

    @property
    def bootstrap_servers(self) -> str:
        return ",".join(
            f"{broker['hostname']}:{broker['internal_port']}" for broker in self.brokers
        )

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "KafkaSettings":
        environment = config["environment"]
        brokers = tuple(environment["kafka_brokers"])
        consumer_config = environment.get("kafka_consumer", {})
        transaction_config = environment.get("kafka_transaction_batch", {})
        topic_config = environment.get("kafka_topics", {})

        return cls(
            brokers=brokers,
            consumer_max_poll_interval_ms=int(
                consumer_config.get("max_poll_interval_ms", 1_800_000)
            ),
            max_record_bytes=int(
                os.getenv("KAFKA_MAX_RECORD_BYTES")
                or environment.get("kafka_max_record_bytes", 900_000)
            ),
            transaction_batch_size=int(
                os.getenv("KAFKA_TRANSACTION_BATCH_SIZE")
                or transaction_config.get("size", 100)
            ),
            transaction_batch_timeout_ms=int(
                os.getenv("KAFKA_TRANSACTION_BATCH_TIMEOUT_MS")
                or transaction_config.get("timeout_ms", 50)
            ),
            transaction_commit_timeout_ms=int(
                os.getenv("KAFKA_TRANSACTION_COMMIT_TIMEOUT_MS")
                or transaction_config.get("commit_timeout_ms", 15_000)
            ),
            transaction_timeout_ms=int(
                os.getenv("KAFKA_TRANSACTION_TIMEOUT_MS")
                or transaction_config.get("transaction_timeout_ms", 30_000)
            ),
            topic_default_partitions=int(os.getenv("KAFKA_TOPIC_PARTITIONS") or 12),
            topic_replication_factor=int(
                os.getenv("KAFKA_TOPIC_REPLICATION_FACTOR")
                or topic_config.get("replication_factor", len(brokers) or 1)
            ),
            pipeline_mode=(
                os.getenv("KAFKA_PIPELINE_MODE")
                or environment.get("kafka_pipeline_mode", "exactly_once")
            )
            .strip()
            .lower(),
            topic_stage_config=topic_config.get("stages", {}),
            topic_exact_config=topic_config.get("topics", {}),
            pipeline_topic_prefixes=(
                environment.get("kafka_topics_prefix", {}).get("pipeline", {})
            ),
        )


CONFIG = setup_config()
KAFKA_SETTINGS = KafkaSettings.from_config(CONFIG)
RETRY_SETTINGS: RetrySettings = load_retry_settings(CONFIG)

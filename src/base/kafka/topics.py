"""Kafka topic configuration, provisioning, and consumer-group naming."""

import os

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic

from src.base.kafka import config as kafka_config
from src.base.log_config import get_logger
from src.base.retry import retry_forever

logger = get_logger()


def normalize_topics(topics: str | list[str]) -> list[str]:
    if isinstance(topics, str):
        return [topics]
    return topics


def _as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _topic_config(topic: str | None) -> dict:
    if topic is None:
        return {}

    exact_config = kafka_config.KAFKA_TOPIC_EXACT_CONFIG.get(topic)
    if exact_config is not None:
        return exact_config

    matched_stage = None
    matched_prefix_length = -1
    for stage_name, topic_prefix in kafka_config.KAFKA_PIPELINE_TOPIC_PREFIXES.items():
        if not topic_prefix:
            continue
        if topic == topic_prefix or topic.startswith(f"{topic_prefix}-"):
            if len(topic_prefix) > matched_prefix_length:
                matched_stage = stage_name
                matched_prefix_length = len(topic_prefix)

    if matched_stage is None:
        return {}
    return kafka_config.KAFKA_TOPIC_STAGE_CONFIG.get(matched_stage, {})


def _runtime_min_topic_partitions() -> int:
    try:
        return int(os.getenv("KAFKA_TOPIC_MIN_PARTITIONS", "1"))
    except ValueError:
        return 1


def _desired_topic_partitions(
    topic: str | None = None, override: int | None = None
) -> int:
    topic_config = _topic_config(topic)
    configured_partitions = override
    if configured_partitions is None:
        configured_partitions = topic_config.get(
            "partitions", kafka_config.KAFKA_TOPIC_DEFAULT_PARTITIONS
        )
    return max(
        1,
        kafka_config.NUMBER_OF_INSTANCES,
        _runtime_min_topic_partitions(),
        int(configured_partitions),
    )


def _topic_replication_factor(
    topic: str | None = None, override: int | None = None
) -> int:
    broker_count = max(1, len(kafka_config.KAFKA_BROKERS))
    topic_config = _topic_config(topic)
    configured_replication_factor = override
    if configured_replication_factor is None:
        configured_replication_factor = topic_config.get(
            "replication_factor", kafka_config.KAFKA_TOPIC_REPLICATION_FACTOR
        )
    configured_replication_factor = max(1, int(configured_replication_factor))
    return min(configured_replication_factor, broker_count)


def topic_partition_count(cluster_metadata, topic: str) -> int | None:
    topics_metadata = getattr(cluster_metadata, "topics", {})
    if isinstance(topics_metadata, dict):
        topic_metadata = topics_metadata.get(topic)
        if topic_metadata is None:
            return None
        partitions = getattr(topic_metadata, "partitions", None)
        return 1 if partitions is None else len(partitions)
    return 1 if topic in topics_metadata else None


class KafkaTopicManager:
    """Reconcile a set of topics with the configured partition policy."""

    def __init__(self, admin_client: AdminClient) -> None:
        self.admin_client = admin_client

    def ensure(
        self,
        topics: str | list[str],
        target_partitions: int | None = None,
        replication_factor: int | None = None,
        auto_expand_partitions: bool | None = None,
    ) -> dict[str, int]:
        normalized_topics = normalize_topics(topics)
        target_by_topic = {
            topic: _desired_topic_partitions(topic, target_partitions)
            for topic in normalized_topics
        }
        replication_by_topic = {
            topic: _topic_replication_factor(topic, replication_factor)
            for topic in normalized_topics
        }
        should_expand = (
            _as_bool(kafka_config.KAFKA_TOPIC_AUTO_EXPAND_PARTITIONS)
            if auto_expand_partitions is None
            else _as_bool(auto_expand_partitions)
        )

        cluster_metadata = retry_forever(
            lambda: self.admin_client.list_topics(timeout=10),
            "Kafka metadata lookup",
            kafka_config.RETRY_SETTINGS,
        )
        topics_metadata = getattr(cluster_metadata, "topics", {})
        existing_topics = (
            set(topics_metadata.keys())
            if isinstance(topics_metadata, dict)
            else set(topics_metadata)
        )
        missing_topics = [
            topic for topic in normalized_topics if topic not in existing_topics
        ]

        if missing_topics:
            logger.info("Creating Kafka topics %s.", missing_topics)
            retry_forever(
                lambda: self._wait_for_admin_futures(
                    self.admin_client.create_topics(
                        [
                            NewTopic(
                                topic,
                                target_by_topic[topic],
                                replication_by_topic[topic],
                            )
                            for topic in missing_topics
                        ]
                    ),
                    "create topic",
                ),
                f"Kafka topic creation for {missing_topics}",
                kafka_config.RETRY_SETTINGS,
            )

        if not should_expand:
            return target_by_topic

        cluster_metadata = retry_forever(
            lambda: self.admin_client.list_topics(timeout=10),
            "Kafka metadata lookup after topic creation",
            kafka_config.RETRY_SETTINGS,
        )
        topics_to_expand = []
        for topic in normalized_topics:
            current_partition_count = topic_partition_count(cluster_metadata, topic)
            if current_partition_count is None:
                continue
            target = target_by_topic[topic]
            if current_partition_count < target:
                logger.info(
                    "Expanding Kafka topic '%s' from %d to %d partition(s).",
                    topic,
                    current_partition_count,
                    target,
                )
                topics_to_expand.append(NewPartitions(topic, target))

        if topics_to_expand:
            retry_forever(
                lambda: self._wait_for_admin_futures(
                    self.admin_client.create_partitions(topics_to_expand),
                    "expand partitions",
                ),
                "Kafka partition expansion for "
                f"{[str(topic) for topic in topics_to_expand]}",
                kafka_config.RETRY_SETTINGS,
            )

        return target_by_topic

    @staticmethod
    def _wait_for_admin_futures(futures: dict, operation: str) -> None:
        for topic, future in futures.items():
            try:
                future.result()
            except KafkaException as exception:
                if operation == "create topic" and _is_topic_already_created(exception):
                    logger.info("Kafka topic '%s' already exists.", topic)
                    continue
                if (
                    operation == "expand partitions"
                    and _is_partition_count_already_satisfied(exception)
                ):
                    logger.info(
                        "Kafka topic '%s' already has enough partitions.", topic
                    )
                    continue
                raise


def _is_topic_already_created(exception: Exception) -> bool:
    kafka_error = exception.args[0] if getattr(exception, "args", None) else None
    topic_already_exists_code = getattr(KafkaError, "TOPIC_ALREADY_EXISTS", None)
    if (
        topic_already_exists_code is not None
        and hasattr(kafka_error, "code")
        and kafka_error.code() == topic_already_exists_code
    ):
        return True
    return "already exists" in str(exception).lower()


def _is_partition_count_already_satisfied(exception: Exception) -> bool:
    message = str(exception).lower()
    return "already has" in message or "smaller than current" in message


def ensure_topics(
    admin_client: AdminClient,
    topics: str | list[str],
    target_partitions: int | None = None,
    replication_factor: int | None = None,
    auto_expand_partitions: bool | None = None,
) -> dict[str, int]:
    """Preserve the existing functional API around the topic manager."""
    return KafkaTopicManager(admin_client).ensure(
        topics,
        target_partitions,
        replication_factor,
        auto_expand_partitions,
    )


def _sanitize_consumer_group_part(value: str) -> str:
    return "".join(
        character if character.isalnum() or character in "._-" else "_"
        for character in value
    )


def build_consumer_group_id(topics: str | list[str]) -> str:
    normalized_topics = sorted(normalize_topics(topics))
    topic_suffix = "__".join(
        _sanitize_consumer_group_part(topic) for topic in normalized_topics
    )
    if not topic_suffix:
        return kafka_config.CONSUMER_GROUP_ID
    return f"{kafka_config.CONSUMER_GROUP_ID}.{topic_suffix}"

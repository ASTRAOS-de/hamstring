"""Kafka topic provisioning and stable client identity helpers."""

import os
import re
from collections.abc import Callable

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from src.base.kafka.config import KAFKA_SETTINGS, RETRY_SETTINGS, KafkaSettings
from src.base.kafka.resilience import is_retriable_kafka_exception
from src.base.log_config import get_logger
from src.base.retry import retry_forever

logger = get_logger("base.kafka.topics")


class KafkaTopicNotVisibleError(RuntimeError):
    """Raised while a newly created topic is not yet visible to clients."""


def normalize_topics(topics: str | list[str]) -> list[str]:
    return [topics] if isinstance(topics, str) else list(topics)


def build_transactional_id(
    stage: str,
    consume_topic: str,
    instance_name: str | None = None,
    worker_id: str | None = None,
) -> str:
    """Build the stable, unique Kafka transactional ID for one worker."""
    deployment_id = os.getenv(
        "KAFKA_TRANSACTIONAL_ID_PREFIX",
        os.getenv("HOSTNAME", "default_tid"),
    )
    return ".".join(
        part
        for part in (
            deployment_id,
            stage,
            instance_name,
            consume_topic,
            worker_id or "default",
        )
        if part
    )


def build_consumer_group_id(
    topics: str | list[str], consumer_group_id: str | None = None
) -> str:
    """Build a group ID scoped to the exact set of subscribed topics."""
    group_id = consumer_group_id or os.getenv("GROUP_ID", "default_gid")
    topic_suffix = "__".join(
        re.sub(r"[^A-Za-z0-9._-]", "_", topic)
        for topic in sorted(normalize_topics(topics))
    )
    return f"{group_id}.{topic_suffix}" if topic_suffix else group_id


class KafkaTopicManager:
    """Ensure configured topics exist without imposing partition coupling."""

    def __init__(
        self,
        admin_client: AdminClient,
        settings: KafkaSettings = KAFKA_SETTINGS,
        metadata_client_factory: Callable[[], AdminClient] | None = None,
    ) -> None:
        self.admin_client = admin_client
        self.settings = settings
        self._metadata_client_factory = metadata_client_factory or (
            lambda: AdminClient({"bootstrap.servers": settings.bootstrap_servers})
        )

    def ensure(
        self,
        topics: str | list[str],
        target_partitions: int | None = None,
        replication_factor: int | None = None,
    ) -> None:
        normalized_topics = normalize_topics(topics)
        partitions = {
            topic: self._partitions(topic, target_partitions)
            for topic in normalized_topics
        }
        replication_factors = {
            topic: self._replication_factor(topic, replication_factor)
            for topic in normalized_topics
        }

        cluster_metadata = retry_forever(
            lambda: self.admin_client.list_topics(timeout=10),
            "Kafka metadata lookup",
            RETRY_SETTINGS,
            retry_if=is_retriable_kafka_exception,
        )
        existing_topics = set(cluster_metadata.topics)
        missing_topics = [
            topic for topic in normalized_topics if topic not in existing_topics
        ]
        visibility_requirements = {
            topic: partitions[topic] if topic in missing_topics else 1
            for topic in normalized_topics
        }
        topics_awaiting_visibility = [
            topic
            for topic in normalized_topics
            if self._visible_partition_count(cluster_metadata, topic)
            < visibility_requirements[topic]
        ]

        if missing_topics:
            logger.info("Creating Kafka topics %s.", missing_topics)
            retry_forever(
                lambda: self._wait_for_creation(
                    self.admin_client.create_topics(
                        [
                            NewTopic(
                                topic,
                                partitions[topic],
                                replication_factors[topic],
                            )
                            for topic in missing_topics
                        ]
                    )
                ),
                f"Kafka topic creation for {missing_topics}",
                RETRY_SETTINGS,
                retry_if=is_retriable_kafka_exception,
            )

        if not topics_awaiting_visibility:
            return

        retry_forever(
            lambda: self._verify_visibility(
                topics_awaiting_visibility, visibility_requirements
            ),
            f"Kafka topic visibility for {topics_awaiting_visibility}",
            RETRY_SETTINGS,
            retryable=(KafkaTopicNotVisibleError, KafkaException, OSError),
            retry_if=self._is_visibility_retryable,
        )

    def _topic_config(self, topic: str) -> dict:
        exact_config = self.settings.topic_exact_config.get(topic)
        if exact_config is not None:
            return exact_config

        matching_stages = [
            (len(prefix), stage_name)
            for stage_name, prefix in self.settings.pipeline_topic_prefixes.items()
            if prefix and (topic == prefix or topic.startswith(f"{prefix}-"))
        ]
        if not matching_stages:
            return {}
        _, stage_name = max(matching_stages, key=lambda match: match[0])
        return self.settings.topic_stage_config.get(stage_name, {})

    def _partitions(self, topic: str, override: int | None) -> int:
        configured = (
            override
            if override is not None
            else self._topic_config(topic).get(
                "partitions", self.settings.topic_default_partitions
            )
        )
        return max(1, int(configured))

    def _replication_factor(self, topic: str, override: int | None) -> int:
        configured = (
            override
            if override is not None
            else self._topic_config(topic).get(
                "replication_factor", self.settings.topic_replication_factor
            )
        )
        return min(max(1, int(configured)), max(1, len(self.settings.brokers)))

    @staticmethod
    def _wait_for_creation(futures: dict) -> None:
        for topic, future in futures.items():
            try:
                future.result()
            except KafkaException as exception:
                error = exception.args[0] if exception.args else None
                if (
                    error is not None
                    and error.code() == KafkaError.TOPIC_ALREADY_EXISTS
                ):
                    logger.info("Kafka topic '%s' already exists.", topic)
                    continue
                raise

    def _verify_visibility(
        self,
        topics: list[str],
        expected_partitions: dict[str, int],
    ) -> None:
        # A client that issued create_topics() can retain the metadata snapshot
        # from before creation while the controller is assigning leaders. Use a
        # fresh client for each retry so this barrier observes broker metadata,
        # rather than a stale client-local view.
        cluster_metadata = self._metadata_client_factory().list_topics(timeout=10)
        unavailable_topics = []
        for topic in topics:
            visible_partitions = self._visible_partition_count(
                cluster_metadata, topic
            )
            if visible_partitions < expected_partitions[topic]:
                unavailable_topics.append(
                    f"{topic} ({visible_partitions}/{expected_partitions[topic]} partitions)"
                )

        if unavailable_topics:
            raise KafkaTopicNotVisibleError(
                "Kafka topic metadata is not visible yet: "
                + ", ".join(unavailable_topics)
            )

    @staticmethod
    def _visible_partition_count(cluster_metadata, topic: str) -> int:
        topic_metadata = cluster_metadata.topics.get(topic)
        return len(topic_metadata.partitions) if topic_metadata is not None else 0

    @staticmethod
    def _is_visibility_retryable(exception: BaseException) -> bool:
        return isinstance(
            exception, KafkaTopicNotVisibleError
        ) or is_retriable_kafka_exception(exception)


def ensure_topics(
    topics: str | list[str],
    target_partitions: int | None = None,
    replication_factor: int | None = None,
    settings: KafkaSettings = KAFKA_SETTINGS,
) -> None:
    """Provision topics without exposing the Kafka admin client to a stage."""
    admin_client = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    KafkaTopicManager(admin_client, settings).ensure(
        topics,
        target_partitions=target_partitions,
        replication_factor=replication_factor,
    )

"""Kafka consumer hierarchy shared by simple and exactly-once pipelines."""

import time
from collections.abc import Sequence

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient

from src.base.kafka.config import KAFKA_SETTINGS, RETRY_SETTINGS, KafkaSettings
from src.base.kafka.records import ConsumedKafkaMessage
from src.base.kafka.resilience import (
    is_retriable_kafka_error,
    is_retriable_kafka_exception,
)
from src.base.kafka.topics import (
    KafkaTopicManager,
    build_consumer_group_id,
    normalize_topics,
)
from src.base.log_config import get_logger
from src.base.retry import retry_forever

logger = get_logger("base.kafka.consumer")


class KafkaMessageFetchException(Exception):
    """Raised when Kafka returns a permanent consumer error."""


class KafkaConsumeHandler:
    """Common consumer implementation for both pipeline delivery modes."""

    def __init__(
        self,
        topics: str | list[str],
        settings: KafkaSettings = KAFKA_SETTINGS,
    ) -> None:
        self.settings = settings
        self.topics = normalize_topics(topics)
        self.conf = self._build_consumer_conf()
        self.consumer = self._connect_consumer()

    def _build_consumer_conf(self) -> dict:
        return {
            "bootstrap.servers": self.settings.bootstrap_servers,
            "group.id": build_consumer_group_id(self.topics),
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
            "max.poll.interval.ms": self.settings.consumer_max_poll_interval_ms,
            "fetch.message.max.bytes": self.settings.max_record_bytes,
        }

    def consume_batch(
        self,
        max_messages: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[ConsumedKafkaMessage]:
        """Fetch a bounded group of records without committing their offsets."""
        configured_batch_size = (
            self.settings.transaction_batch_size
            if max_messages is None
            else max_messages
        )
        batch_size = max(1, configured_batch_size)
        batch_timeout_ms = (
            self.settings.transaction_batch_timeout_ms
            if timeout_ms is None
            else max(0, timeout_ms)
        )
        deadline = time.monotonic() + batch_timeout_ms / 1000
        consumed_messages = []

        while len(consumed_messages) < batch_size:
            remaining_timeout = max(0, deadline - time.monotonic())
            try:
                messages = self.consumer.consume(
                    num_messages=batch_size - len(consumed_messages),
                    timeout=remaining_timeout,
                )
            except (KafkaException, OSError) as exception:
                if not is_retriable_kafka_exception(exception):
                    raise KafkaMessageFetchException(
                        f"Kafka consumer batch fetch failed: {exception}"
                    ) from exception
                logger.warning(
                    "Kafka consumer batch fetch failed, reconnecting: %s", exception
                )
                self._reset_consumer()
                return []

            for message in messages or []:
                record = self._to_record(message)
                if record is not None:
                    consumed_messages.append(record)

            if not messages or time.monotonic() >= deadline:
                break

        return consumed_messages

    def consume_one(self) -> ConsumedKafkaMessage:
        """Block until one source record is available."""
        while True:
            records = self.consume_batch(max_messages=1, timeout_ms=1000)
            if records:
                return records[0]

    def commit(self, consumed_messages: Sequence[ConsumedKafkaMessage]) -> None:
        """Synchronously commit explicitly supplied source records."""
        if not consumed_messages:
            return
        offsets = self.offsets_for(consumed_messages)
        retry_forever(
            lambda: self.consumer.commit(offsets=offsets, asynchronous=False),
            "Kafka consumer offset commit",
            RETRY_SETTINGS,
            retryable=(KafkaException, OSError),
            retry_if=is_retriable_kafka_exception,
        )

    @staticmethod
    def offsets_for(
        consumed_messages: Sequence[ConsumedKafkaMessage],
    ) -> list[TopicPartition]:
        """Return the highest processed next offset for every source partition."""
        offsets_by_partition: dict[tuple[str, int], int] = {}
        for message in consumed_messages:
            partition_key = (message.topic, message.partition)
            offsets_by_partition[partition_key] = max(
                offsets_by_partition.get(partition_key, 0), message.offset + 1
            )
        return [
            TopicPartition(topic, partition, offset)
            for (topic, partition), offset in offsets_by_partition.items()
        ]

    def group_metadata(self):
        return self.consumer.consumer_group_metadata()

    def close(self) -> None:
        self.consumer.close()

    def _connect_consumer(self) -> Consumer:
        def connect() -> Consumer:
            admin_client = AdminClient(
                {"bootstrap.servers": self.settings.bootstrap_servers}
            )
            KafkaTopicManager(admin_client, self.settings).ensure(self.topics)
            consumer = Consumer(self.conf)
            consumer.subscribe(self.topics)
            return consumer

        return retry_forever(
            connect,
            f"Kafka consumer setup for {self.topics}",
            RETRY_SETTINGS,
            retryable=(KafkaException, OSError),
            retry_if=is_retriable_kafka_exception,
        )

    def _reset_consumer(self) -> None:
        try:
            self.consumer.close()
        except Exception as exception:
            logger.warning(
                "Ignoring Kafka consumer close failure during reconnect: %s", exception
            )
        self.consumer = self._connect_consumer()

    @staticmethod
    def _to_record(message) -> ConsumedKafkaMessage | None:
        if message is None:
            return None
        error = message.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                return None
            if is_retriable_kafka_error(error):
                logger.warning("Kafka batch fetch received retriable error: %s", error)
                return None
            raise KafkaMessageFetchException(f"Kafka consumer error: {error}")

        return ConsumedKafkaMessage(
            key=message.key().decode("utf-8") if message.key() else None,
            value=message.value().decode("utf-8") if message.value() else None,
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            headers=tuple(message.headers() or ()),
        )


class SimpleKafkaConsumeHandler(KafkaConsumeHandler):
    """Consumer used by the synchronous at-least-once pipeline."""


class ExactlyOnceKafkaConsumeHandler(KafkaConsumeHandler):
    """Consumer that exposes only committed transactional records."""

    def _build_consumer_conf(self) -> dict:
        conf = super()._build_consumer_conf()
        conf["isolation.level"] = "read_committed"
        return conf

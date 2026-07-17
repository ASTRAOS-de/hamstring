"""Kafka consumer hierarchy and offset management."""

import time
from abc import abstractmethod
from collections.abc import Sequence
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient

from src.base.kafka import config as kafka_config
from src.base.kafka.client import KafkaHandler
from src.base.kafka.errors import (
    KafkaMessageFetchException,
    TooManyFailedAttemptsError,
)
from src.base.kafka.records import ConsumedKafkaMessage
from src.base.kafka.resilience import is_retriable_kafka_error
from src.base.kafka.serialization import KafkaSerializationMixin
from src.base.kafka.topics import (
    build_consumer_group_id,
    ensure_topics,
    normalize_topics,
    topic_partition_count,
)
from src.base.log_config import get_logger
from src.base.retry import retry_forever

logger = get_logger()


class KafkaConsumeHandler(KafkaSerializationMixin, KafkaHandler):
    """Common connection, batching, decoding, and offset behavior."""

    def __init__(self, topics: str | list[str]) -> None:
        super().__init__()
        self._last_consumed_message = None
        self.topics = normalize_topics(topics)
        self.brokers = kafka_config.bootstrap_servers()
        self.conf = self._build_consumer_conf()
        self._connect_consumer()

    def _build_consumer_conf(self) -> dict:
        return {
            "bootstrap.servers": self.brokers,
            "group.id": build_consumer_group_id(self.topics),
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
            "max.poll.interval.ms": kafka_config.KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS,
        }

    def _connect_consumer(self) -> None:
        def connect():
            consumer = Consumer(self.conf)
            admin_client = AdminClient({"bootstrap.servers": self.brokers})
            target_partitions_by_topic = ensure_topics(admin_client, self.topics)
            if not self._all_topics_created(
                self.topics, target_partitions_by_topic, consumer
            ):
                try:
                    consumer.close()
                except Exception:
                    pass
                raise TooManyFailedAttemptsError("Not all topics were created.")
            consumer.subscribe(self.topics)
            return consumer

        self.consumer = retry_forever(
            connect,
            f"Kafka consumer setup for {self.topics}",
            kafka_config.RETRY_SETTINGS,
        )

    def _reset_consumer(self) -> None:
        try:
            if self.consumer:
                self.consumer.close()
        except Exception as exception:
            logger.warning(
                "Ignoring Kafka consumer close failure during reconnect: %s",
                exception,
            )
        self._last_consumed_message = None
        self._connect_consumer()

    def commit(
        self,
        consumed_messages: Sequence[ConsumedKafkaMessage] | None = None,
    ) -> None:
        """Commit an explicit batch or the latest record from ``consume``."""
        if not self.consumer:
            return

        if consumed_messages is not None:
            if not consumed_messages:
                return
            retry_forever(
                lambda: self.consumer.commit(
                    offsets=self.offsets_for(consumed_messages),
                    asynchronous=False,
                ),
                "Kafka consumer batch offset commit",
                kafka_config.RETRY_SETTINGS,
                retryable=(KafkaException, RuntimeError, OSError),
            )
            self._last_consumed_message = None
            return

        if self._last_consumed_message is not None:
            retry_forever(
                lambda: self.consumer.commit(self._last_consumed_message),
                "Kafka consumer offset commit",
                kafka_config.RETRY_SETTINGS,
                retryable=(KafkaException, RuntimeError, OSError),
            )
            self._last_consumed_message = None

    def consume_batch(
        self,
        max_messages: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[ConsumedKafkaMessage]:
        """Fetch a bounded group of records without committing offsets."""
        batch_size = max(
            1,
            (
                kafka_config.KAFKA_TRANSACTION_BATCH_SIZE
                if max_messages is None
                else int(max_messages)
            ),
        )
        batch_timeout_ms = max(
            0,
            (
                kafka_config.KAFKA_TRANSACTION_BATCH_TIMEOUT_MS
                if timeout_ms is None
                else int(timeout_ms)
            ),
        )
        deadline = time.monotonic() + batch_timeout_ms / 1000
        records = []

        while len(records) < batch_size:
            timeout = max(0.0, deadline - time.monotonic())
            try:
                messages = self.consumer.consume(
                    num_messages=batch_size - len(records),
                    timeout=timeout,
                )
            except (KafkaException, RuntimeError, OSError) as exception:
                logger.warning(
                    "Kafka consumer batch fetch failed, reconnecting: %s",
                    exception,
                )
                self._reset_consumer()
                return []

            for message in messages or []:
                record = self._record_from_message(message)
                if record is not None:
                    records.append(record)
            if not messages or time.monotonic() >= deadline:
                break

        if records:
            self._last_consumed_message = records[-1].raw_message
        return records

    @staticmethod
    def offsets_for(
        consumed_messages: Sequence[ConsumedKafkaMessage],
    ) -> list[TopicPartition]:
        """Return the highest processed next offset for each source partition."""
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

    @abstractmethod
    def consume(self, *args, **kwargs):
        raise NotImplementedError

    def _all_topics_created(
        self,
        topics: list[str],
        min_partitions: int | dict[str, int] = 1,
        consumer=None,
    ) -> bool:
        number_of_retries_left = 30
        all_topics_created = False
        consumer = consumer or self.consumer
        while not all_topics_created:
            assigned_topics = retry_forever(
                lambda: consumer.list_topics(timeout=10),
                "Kafka topic visibility check",
                kafka_config.RETRY_SETTINGS,
                retryable=(KafkaException, RuntimeError, OSError),
            )
            all_topics_created = True
            for topic in topics:
                partition_count = topic_partition_count(assigned_topics, topic)
                required_partitions = (
                    min_partitions.get(topic, 1)
                    if isinstance(min_partitions, dict)
                    else min_partitions
                )
                if partition_count is None or partition_count < required_partitions:
                    all_topics_created = False

            if not all_topics_created:
                number_of_retries_left -= 1
            if number_of_retries_left <= 0:
                return False
            time.sleep(0.5)
        return True

    def _poll_message(self):
        while True:
            try:
                message = self.consumer.poll(timeout=1.0)
            except (KafkaException, RuntimeError, OSError) as exception:
                logger.warning(
                    "Kafka consumer poll failed, reconnecting: %s", exception
                )
                self._reset_consumer()
                continue

            if message is None:
                return None
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    return None
                if is_retriable_kafka_error(message.error()):
                    logger.warning(
                        "Kafka consumer error is retriable, reconnecting: %s",
                        message.error(),
                    )
                    self._reset_consumer()
                    return None
            return message

    @staticmethod
    def _record_from_message(message) -> ConsumedKafkaMessage | None:
        if message is None:
            return None
        error = message.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                return None
            if is_retriable_kafka_error(error):
                logger.warning("Kafka consumer received retriable error: %s", error)
                return None
            raise KafkaMessageFetchException(f"Kafka consumer error: {error}")

        return ConsumedKafkaMessage(
            key=message.key().decode("utf-8") if message.key() else None,
            value=message.value().decode("utf-8") if message.value() else None,
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            raw_message=message,
        )

    def _consume_single(self, shutdown_message: str):
        empty_data_retrieved = False
        try:
            while True:
                message = self._poll_message()
                if message is None:
                    if not empty_data_retrieved:
                        logger.info("Waiting for messages...")
                    empty_data_retrieved = True
                    continue
                if message.error():
                    logger.error("Consumer error: %s", message.error())
                    raise ValueError("Message is invalid")

                key = message.key().decode("utf-8") if message.key() else None
                value = message.value().decode("utf-8") if message.value() else None
                topic = message.topic() if message.topic() else None
                self._last_consumed_message = message
                return key, value, topic
        except KeyboardInterrupt:
            logger.info(shutdown_message)

    def __del__(self) -> None:
        if getattr(self, "consumer", None):
            self.consumer.close()


class SimpleKafkaConsumeHandler(KafkaConsumeHandler):
    """Consumer without transactional read isolation."""

    def consume(self) -> tuple[Optional[str], Optional[str], Optional[str]]:
        return self._consume_single("Stopping KafkaConsumeHandler...")


class ExactlyOnceKafkaConsumeHandler(KafkaConsumeHandler):
    """Consumer that exposes only committed transactional records."""

    def _build_consumer_conf(self) -> dict:
        conf = super()._build_consumer_conf()
        conf["isolation.level"] = "read_committed"
        return conf

    def consume(self) -> tuple[Optional[str], Optional[str], Optional[str]]:
        return self._consume_single("Shutting down KafkaConsumeHandler...")

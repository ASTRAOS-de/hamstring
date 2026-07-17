"""Kafka producer hierarchy for simple, buffered, and EoS delivery."""

import time
import uuid
from abc import abstractmethod
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Callable

from confluent_kafka import KafkaException, Producer

from src.base.kafka import config as kafka_config
from src.base.kafka.client import KafkaHandler
from src.base.kafka.records import ConsumedKafkaMessage, KafkaProduceRecord
from src.base.kafka.resilience import (
    is_retriable_kafka_error,
    is_retriable_kafka_exception,
)
from src.base.log_config import get_logger
from src.base.retry import retry_forever
from src.base.utils import kafka_delivery_report

if TYPE_CHECKING:
    from src.base.kafka.consumer import KafkaConsumeHandler

logger = get_logger()


class KafkaProduceHandler(KafkaHandler):
    """Common lifecycle and retry behavior for Kafka producers."""

    def __init__(self, conf):
        super().__init__()
        self.conf = conf
        self.producer = self._new_producer()

    def _new_producer(self):
        return retry_forever(
            lambda: Producer(self.conf),
            "Kafka producer creation",
            kafka_config.RETRY_SETTINGS,
        )

    def _reset_producer(self) -> None:
        try:
            if self.producer:
                self.producer.flush(5)
        except Exception as exception:
            logger.warning(
                "Ignoring Kafka producer flush failure during reconnect: %s",
                exception,
            )
        self.producer = self._new_producer()

    def _with_producer_retry(
        self, description: str, operation: Callable[[], None]
    ) -> None:
        def attempt():
            try:
                operation()
            except Exception as exception:
                if not is_retriable_kafka_exception(exception):
                    raise
                logger.warning(
                    "%s failed, recreating Kafka producer: %s",
                    description,
                    exception,
                )
                self._reset_producer()
                raise

        retry_forever(
            attempt,
            description,
            kafka_config.RETRY_SETTINGS,
            retryable=(KafkaException, BufferError, RuntimeError, OSError),
        )

    @abstractmethod
    def produce(self, *args, **kwargs):
        raise NotImplementedError

    def __del__(self) -> None:
        if getattr(self, "producer", None):
            self.producer.flush()


class SimpleKafkaProduceHandler(KafkaProduceHandler):
    """Synchronous Kafka producer without transactional semantics."""

    def __init__(self):
        self.brokers = kafka_config.bootstrap_servers()
        super().__init__(
            {
                "bootstrap.servers": self.brokers,
                "enable.idempotence": False,
                "acks": "1",
                "message.max.bytes": 1_000_000_000,
            }
        )

    def produce(self, topic: str, data: str, key: None | str = None) -> None:
        if not data:
            return

        def operation():
            delivery_errors = []

            def delivery_callback(error, message):
                kafka_delivery_report(error, message)
                if error:
                    delivery_errors.append(error)

            self.producer.flush()
            self.producer.produce(
                topic=topic,
                key=key,
                value=data,
                callback=delivery_callback,
            )
            self.producer.flush()
            if delivery_errors:
                delivery_error = delivery_errors[0]
                if is_retriable_kafka_error(delivery_error):
                    raise KafkaException(delivery_error)
                raise ValueError(f"Kafka delivery failed: {delivery_error}")

        self._with_producer_retry(f"Kafka produce to {topic}", operation)


class BufferedKafkaProduceHandler(SimpleKafkaProduceHandler):
    """Asynchronous producer with bounded local backpressure."""

    _QUEUE_POLL_TIMEOUT_SECONDS = 0.1

    def __init__(self):
        self.brokers = kafka_config.bootstrap_servers()
        KafkaProduceHandler.__init__(
            self,
            {
                "bootstrap.servers": self.brokers,
                "enable.idempotence": False,
                "acks": "1",
                "message.max.bytes": 1_000_000_000,
                "linger.ms": 10,
                "batch.num.messages": 1000,
                "queue.buffering.max.messages": 10000,
            },
        )

    def produce(self, topic: str, data: str, key: None | str = None) -> None:
        if not data:
            return

        def delivery_callback(error, message):
            kafka_delivery_report(error, message)

        def operation():
            queue_was_full = False
            while True:
                self.producer.poll(0)
                try:
                    self.producer.produce(
                        topic=topic,
                        key=key,
                        value=data,
                        callback=delivery_callback,
                    )
                    return
                except BufferError:
                    if not queue_was_full:
                        logger.warning(
                            "Kafka telemetry producer queue is full; "
                            "waiting for delivery reports."
                        )
                        queue_was_full = True
                    self.producer.poll(self._QUEUE_POLL_TIMEOUT_SECONDS)

        self._with_producer_retry(f"Buffered Kafka produce to {topic}", operation)


class ExactlyOnceKafkaProduceHandler(KafkaProduceHandler):
    """Transactional producer that atomically commits outputs and input offsets."""

    def __init__(self):
        self._transaction_records: list[KafkaProduceRecord] | None = None
        self.brokers = kafka_config.bootstrap_servers()
        super().__init__(
            {
                "bootstrap.servers": self.brokers,
                "transactional.id": f"{kafka_config.HOSTNAME}-{uuid.uuid4()}",
                "enable.idempotence": True,
                "message.max.bytes": 1_000_000_000,
            }
        )
        self._init_transactions_with_retry()

    def _reset_producer(self) -> None:
        super()._reset_producer()
        self._init_transactions_with_retry()

    def _init_transactions_with_retry(self) -> None:
        retry_forever(
            lambda: self.producer.init_transactions(),
            "Kafka transactional producer initialization",
            kafka_config.RETRY_SETTINGS,
        )

    def produce(self, topic: str, data: str, key: None | str = None) -> None:
        if not data:
            return

        record = KafkaProduceRecord(topic=topic, data=data, key=key)
        if self._transaction_records is not None:
            self._transaction_records.append(record)
            return
        self._run_transaction([record])

    @contextmanager
    def transaction_batch(
        self,
        consumer: "KafkaConsumeHandler",
        consumed_messages: Sequence[ConsumedKafkaMessage],
    ) -> Iterator[None]:
        """Collect outputs and commit them with source offsets on clean exit."""
        if self._transaction_records is not None:
            raise RuntimeError("Kafka transaction batches cannot be nested.")
        if not consumed_messages:
            raise ValueError("A Kafka transaction batch requires source messages.")

        self._transaction_records = []
        try:
            yield
            records = self._transaction_records
        except Exception:
            self._transaction_records = None
            raise
        else:
            self._transaction_records = None
            self._run_transaction(
                records,
                consumer=consumer,
                consumed_messages=consumed_messages,
            )

    def _run_transaction(
        self,
        records: Sequence[KafkaProduceRecord],
        consumer: "KafkaConsumeHandler | None" = None,
        consumed_messages: Sequence[ConsumedKafkaMessage] = (),
    ) -> None:
        def operation():
            self.producer.begin_transaction()
            try:
                for record in records:
                    if not record.data:
                        continue
                    self.producer.produce(
                        topic=record.topic,
                        key=record.key,
                        value=record.data,
                        callback=kafka_delivery_report,
                    )
                if consumer is not None:
                    self.producer.send_offsets_to_transaction(
                        consumer.offsets_for(consumed_messages),
                        consumer.group_metadata(),
                    )
                self.commit_transaction_with_retry()
            except Exception as exception:
                logger.info("Aborting Kafka transaction.")
                try:
                    self.producer.abort_transaction()
                except Exception as abort_exception:
                    logger.warning(
                        "Kafka transaction abort failed: %s", abort_exception
                    )
                logger.error("Transaction aborted.")
                logger.error(exception)
                raise

        self._with_producer_retry("Kafka transaction", operation)

    def commit_transaction_with_retry(
        self, max_retries: int = 3, retry_interval_ms: int = 1000
    ) -> None:
        committed = False
        retry_count = 0
        while not committed and retry_count < max_retries:
            try:
                self.producer.commit_transaction()
                committed = True
            except KafkaException as exception:
                if (
                    "Conflicting commit_transaction API call is already in progress"
                    in str(exception)
                ):
                    retry_count += 1
                    logger.debug(
                        "Conflicting commit_transaction API call is already in "
                        "progress: Retrying"
                    )
                    time.sleep(retry_interval_ms / 1000.0)
                else:
                    raise

        if not committed:
            raise RuntimeError("Failed to commit transaction after retries.")

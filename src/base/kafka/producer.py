"""Kafka producer hierarchy with one pipeline completion contract."""

from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from confluent_kafka import KafkaError, KafkaException, Producer

from src.base.kafka.config import KAFKA_SETTINGS, RETRY_SETTINGS, KafkaSettings
from src.base.kafka.records import ConsumedKafkaMessage, KafkaProduceRecord
from src.base.kafka.resilience import (
    is_retriable_kafka_error,
    is_retriable_kafka_exception,
)
from src.base.log_config import get_logger
from src.base.retry import retry_forever

if TYPE_CHECKING:
    from src.base.kafka.consumer import KafkaConsumeHandler

logger = get_logger("base.kafka.producer")


def _log_delivery_report(error, message) -> None:
    if error:
        if error.code() in {KafkaError._PURGE_QUEUE, KafkaError._PURGE_INFLIGHT}:
            logger.debug("Message removed while aborting Kafka transaction: %s", error)
            return
        logger.warning("Message delivery failed: %s", error)
    else:
        logger.debug(
            "Message delivered to topic=%s [partition=%s]",
            message.topic(),
            message.partition(),
        )


class KafkaProduceHandler(ABC):
    """Common producer interface for simple and exactly-once delivery."""

    def __init__(self, producer_config: dict) -> None:
        self.producer_config = producer_config
        self.producer = self._new_producer()

    @abstractmethod
    def publish(self, records: Sequence[KafkaProduceRecord]) -> None:
        """Publish records that do not originate from a Kafka consumer."""
        raise NotImplementedError

    @abstractmethod
    def complete(
        self,
        records: Sequence[KafkaProduceRecord],
        consumer: "KafkaConsumeHandler",
        consumed_messages: Sequence[ConsumedKafkaMessage],
    ) -> None:
        """Publish outputs and acknowledge the corresponding source records."""
        raise NotImplementedError

    def close(self, timeout_seconds: float = 10.0) -> None:
        """Explicitly drain the producer during orderly shutdown."""
        self.producer.flush(timeout_seconds)

    def _new_producer(self) -> Producer:
        return retry_forever(
            lambda: Producer(self.producer_config),
            "Kafka producer creation",
            RETRY_SETTINGS,
            retry_if=is_retriable_kafka_exception,
        )

    def _reset_producer(self) -> None:
        try:
            self.producer.flush(5)
        except Exception as exception:
            logger.warning(
                "Ignoring Kafka producer flush failure during reconnect: %s", exception
            )
        self.producer = self._new_producer()

    def _with_producer_retry(
        self, description: str, operation: Callable[[], None]
    ) -> None:
        def attempt() -> None:
            try:
                operation()
            except Exception as exception:
                if not is_retriable_kafka_exception(exception):
                    raise
                logger.warning(
                    "%s failed, recreating Kafka producer: %s", description, exception
                )
                self._reset_producer()
                raise

        retry_forever(
            attempt,
            description,
            RETRY_SETTINGS,
            retryable=(KafkaException, BufferError, OSError),
            retry_if=is_retriable_kafka_exception,
        )


class SimpleKafkaProduceHandler(KafkaProduceHandler):
    """Synchronous at-least-once producer."""

    def __init__(self, settings: KafkaSettings = KAFKA_SETTINGS) -> None:
        self.settings = settings
        super().__init__(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "message.max.bytes": settings.max_record_bytes,
            }
        )

    def publish(self, records: Sequence[KafkaProduceRecord]) -> None:
        publishable_records = [record for record in records if record.data]
        if not publishable_records:
            return

        def operation() -> None:
            delivery_errors = []

            def delivery_callback(error, message) -> None:
                _log_delivery_report(error, message)
                if error:
                    delivery_errors.append(error)

            for record in publishable_records:
                self.producer.produce(**record.produce_kwargs(delivery_callback))

            remaining_records = self.producer.flush()
            if remaining_records:
                raise TimeoutError(
                    f"Kafka flush timed out with {remaining_records} "
                    "undelivered record(s)."
                )
            if delivery_errors:
                delivery_error = delivery_errors[0]
                if is_retriable_kafka_error(delivery_error):
                    raise KafkaException(delivery_error)
                raise ValueError(f"Kafka delivery failed: {delivery_error}")

        self._with_producer_retry("Kafka record publication", operation)

    def complete(
        self,
        records: Sequence[KafkaProduceRecord],
        consumer: "KafkaConsumeHandler",
        consumed_messages: Sequence[ConsumedKafkaMessage],
    ) -> None:
        if not consumed_messages:
            raise ValueError("Completing Kafka work requires source records.")
        self.publish(records)
        consumer.commit(consumed_messages)


class BufferedKafkaProduceHandler(KafkaProduceHandler):
    """Asynchronous telemetry producer with bounded local backpressure."""

    _QUEUE_POLL_TIMEOUT_SECONDS = 0.1

    def __init__(self, settings: KafkaSettings = KAFKA_SETTINGS) -> None:
        self.settings = settings
        super().__init__(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "enable.idempotence": False,
                "acks": "1",
                "message.max.bytes": settings.max_record_bytes,
                "linger.ms": 10,
                "batch.num.messages": 1000,
                "queue.buffering.max.messages": 10000,
            }
        )

    def publish(self, records: Sequence[KafkaProduceRecord]) -> None:
        def operation() -> None:
            queue_was_full = False
            for record in records:
                if not record.data:
                    continue
                while True:
                    self.producer.poll(0)
                    try:
                        self.producer.produce(
                            **record.produce_kwargs(_log_delivery_report)
                        )
                        break
                    except BufferError:
                        if not queue_was_full:
                            logger.warning(
                                "Kafka telemetry producer queue is full; "
                                "waiting for delivery reports."
                            )
                            queue_was_full = True
                        self.producer.poll(self._QUEUE_POLL_TIMEOUT_SECONDS)

        self._with_producer_retry("Buffered Kafka publication", operation)

    def complete(
        self,
        records: Sequence[KafkaProduceRecord],
        consumer: "KafkaConsumeHandler",
        consumed_messages: Sequence[ConsumedKafkaMessage],
    ) -> None:
        raise TypeError("Buffered telemetry producers cannot commit source offsets.")


class ExactlyOnceKafkaProduceHandler(KafkaProduceHandler):
    """Transactional producer that commits outputs and input offsets together."""

    def __init__(
        self,
        transactional_id: str,
        settings: KafkaSettings = KAFKA_SETTINGS,
    ) -> None:
        if not transactional_id:
            raise ValueError("Exactly-once producers require a transactional ID.")

        self.settings = settings
        self.transactional_id = transactional_id
        self.transaction_commit_timeout_seconds = max(
            0.1, settings.transaction_commit_timeout_ms / 1000
        )
        super().__init__(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "transactional.id": transactional_id,
                "enable.idempotence": True,
                "message.max.bytes": settings.max_record_bytes,
                "transaction.timeout.ms": settings.transaction_timeout_ms,
            }
        )
        self._init_transactions_with_retry()

    def publish(self, records: Sequence[KafkaProduceRecord]) -> None:
        publishable_records = [record for record in records if record.data]
        if publishable_records:
            self._run_transaction(publishable_records)

    def complete(
        self,
        records: Sequence[KafkaProduceRecord],
        consumer: "KafkaConsumeHandler",
        consumed_messages: Sequence[ConsumedKafkaMessage],
    ) -> None:
        if not consumed_messages:
            raise ValueError("Completing Kafka work requires source records.")
        self._run_transaction(records, consumer, consumed_messages)

    def _reset_producer(self) -> None:
        super()._reset_producer()
        self._init_transactions_with_retry()

    def _init_transactions_with_retry(self) -> None:
        retry_forever(
            lambda: self.producer.init_transactions(
                self.transaction_commit_timeout_seconds
            ),
            "Kafka transactional producer initialization",
            RETRY_SETTINGS,
            retry_if=is_retriable_kafka_exception,
        )

    def _run_transaction(
        self,
        records: Sequence[KafkaProduceRecord],
        consumer: "KafkaConsumeHandler | None" = None,
        consumed_messages: Sequence[ConsumedKafkaMessage] = (),
    ) -> None:
        def operation() -> None:
            self.producer.begin_transaction()
            try:
                for record in records:
                    if record.data:
                        self.producer.produce(
                            **record.produce_kwargs(_log_delivery_report)
                        )
                if consumer is not None:
                    self.producer.send_offsets_to_transaction(
                        consumer.offsets_for(consumed_messages),
                        consumer.group_metadata(),
                    )
                self.producer.commit_transaction(
                    self.transaction_commit_timeout_seconds
                )
            except Exception:
                logger.info("Aborting Kafka transaction.")
                try:
                    self.producer.abort_transaction(
                        self.transaction_commit_timeout_seconds
                    )
                except Exception as abort_exception:
                    logger.warning(
                        "Kafka transaction abort failed: %s", abort_exception
                    )
                raise

        self._with_producer_retry("Kafka transaction", operation)

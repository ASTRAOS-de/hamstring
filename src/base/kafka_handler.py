"""
The Write-Exactly-Once-Semantics used by the :class:`KafkaHandler` is shown by
https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/eos-transactions.py,
parts of which are similar to the code in this module.
"""

import ast
import json
import os
import sys
import time
import uuid
from abc import abstractmethod
from typing import Callable, Optional

import marshmallow_dataclass
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
)
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic

sys.path.append(os.getcwd())
from src.base.data_classes.batch import Batch
from src.base.log_config import get_logger
from src.base.retry import retry_forever
from src.base.utils import kafka_delivery_report, setup_config

logger = get_logger()

HOSTNAME = os.getenv("HOSTNAME", "default_tid")
CONSUMER_GROUP_ID = os.getenv("GROUP_ID", "default_gid")
NUMBER_OF_INSTANCES = int(os.getenv("NUMBER_OF_INSTANCES", 1))

config = setup_config()
KAFKA_BROKERS = config["environment"]["kafka_brokers"]
KAFKA_CONSUMER_CONFIG = config["environment"].get("kafka_consumer", {})
KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS = int(
    KAFKA_CONSUMER_CONFIG.get("max_poll_interval_ms", 1800000)
)
KAFKA_TOPIC_CONFIG = config["environment"].get("kafka_topics", {})
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
    config["environment"].get("kafka_topics_prefix", {}).get("pipeline", {})
)


def _normalize_topics(topics: str | list[str]) -> list[str]:
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

    exact_config = KAFKA_TOPIC_EXACT_CONFIG.get(topic)
    if exact_config is not None:
        return exact_config

    matched_stage = None
    matched_prefix_length = -1
    for stage_name, topic_prefix in KAFKA_PIPELINE_TOPIC_PREFIXES.items():
        if not topic_prefix:
            continue
        if topic == topic_prefix or topic.startswith(f"{topic_prefix}-"):
            if len(topic_prefix) > matched_prefix_length:
                matched_stage = stage_name
                matched_prefix_length = len(topic_prefix)

    if matched_stage is None:
        return {}

    return KAFKA_TOPIC_STAGE_CONFIG.get(matched_stage, {})


def _desired_topic_partitions(
    topic: str | None = None, override: int | None = None
) -> int:
    topic_config = _topic_config(topic)
    configured_partitions = override
    if configured_partitions is None:
        configured_partitions = topic_config.get(
            "partitions", KAFKA_TOPIC_DEFAULT_PARTITIONS
        )
    return max(
        1,
        NUMBER_OF_INSTANCES,
        _runtime_min_topic_partitions(),
        int(configured_partitions),
    )


def _runtime_min_topic_partitions() -> int:
    try:
        return int(os.getenv("KAFKA_TOPIC_MIN_PARTITIONS", "1"))
    except ValueError:
        return 1


def _topic_replication_factor(
    topic: str | None = None, override: int | None = None
) -> int:
    broker_count = max(1, len(KAFKA_BROKERS))
    topic_config = _topic_config(topic)
    configured_replication_factor = override
    if configured_replication_factor is None:
        configured_replication_factor = topic_config.get(
            "replication_factor", KAFKA_TOPIC_REPLICATION_FACTOR
        )
    configured_replication_factor = max(1, int(configured_replication_factor))
    return min(configured_replication_factor, broker_count)


def _topic_partition_count(cluster_metadata, topic: str) -> int | None:
    topics_metadata = getattr(cluster_metadata, "topics", {})

    if isinstance(topics_metadata, dict):
        topic_metadata = topics_metadata.get(topic)
        if topic_metadata is None:
            return None

        partitions = getattr(topic_metadata, "partitions", None)
        if partitions is None:
            return 1
        return len(partitions)

    if topic in topics_metadata:
        return 1

    return None


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
                logger.info("Kafka topic '%s' already has enough partitions.", topic)
                continue
            raise


def _is_retriable_kafka_exception(exception: Exception) -> bool:
    if isinstance(exception, (KafkaException, BufferError, RuntimeError, OSError)):
        return True
    return False


def _is_retriable_kafka_error(error) -> bool:
    retriable = getattr(error, "retriable", None)
    if callable(retriable) and retriable():
        return True

    retriable_codes = {
        getattr(KafkaError, name)
        for name in (
            "_ALL_BROKERS_DOWN",
            "_TRANSPORT",
            "_TIMED_OUT",
            "_MSG_TIMED_OUT",
            "_RESOLVE",
            "_WAIT_COORD",
        )
        if hasattr(KafkaError, name)
    }
    return hasattr(error, "code") and error.code() in retriable_codes


def ensure_topics(
    admin_client: AdminClient,
    topics: str | list[str],
    target_partitions: int | None = None,
    replication_factor: int | None = None,
    auto_expand_partitions: bool | None = None,
) -> dict[str, int]:
    normalized_topics = _normalize_topics(topics)
    target_partitions_by_topic = {
        topic: _desired_topic_partitions(topic, target_partitions)
        for topic in normalized_topics
    }
    replication_factor_by_topic = {
        topic: _topic_replication_factor(topic, replication_factor)
        for topic in normalized_topics
    }
    auto_expand_partitions = (
        _as_bool(KAFKA_TOPIC_AUTO_EXPAND_PARTITIONS)
        if auto_expand_partitions is None
        else _as_bool(auto_expand_partitions)
    )

    cluster_metadata = retry_forever(
        lambda: admin_client.list_topics(timeout=10),
        "Kafka metadata lookup",
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
        logger.info(
            "Creating Kafka topics %s.",
            missing_topics,
        )
        retry_forever(
            lambda: _wait_for_admin_futures(
                admin_client.create_topics(
                    [
                        NewTopic(
                            topic,
                            target_partitions_by_topic[topic],
                            replication_factor_by_topic[topic],
                        )
                        for topic in missing_topics
                    ]
                ),
                "create topic",
            ),
            f"Kafka topic creation for {missing_topics}",
        )

    if not auto_expand_partitions:
        return target_partitions_by_topic

    cluster_metadata = retry_forever(
        lambda: admin_client.list_topics(timeout=10),
        "Kafka metadata lookup after topic creation",
    )
    topics_to_expand = []
    for topic in normalized_topics:
        current_partition_count = _topic_partition_count(cluster_metadata, topic)
        if current_partition_count is None:
            continue
        target_partitions = target_partitions_by_topic[topic]
        if current_partition_count < target_partitions:
            logger.info(
                "Expanding Kafka topic '%s' from %d to %d partition(s).",
                topic,
                current_partition_count,
                target_partitions,
            )
            topics_to_expand.append(NewPartitions(topic, target_partitions))

    if topics_to_expand:
        retry_forever(
            lambda: _wait_for_admin_futures(
                admin_client.create_partitions(topics_to_expand),
                "expand partitions",
            ),
            f"Kafka partition expansion for {[str(topic) for topic in topics_to_expand]}",
        )

    return target_partitions_by_topic


def _sanitize_consumer_group_part(value: str) -> str:
    return "".join(
        character if character.isalnum() or character in "._-" else "_"
        for character in value
    )


def build_consumer_group_id(topics: str | list[str]) -> str:
    normalized_topics = sorted(_normalize_topics(topics))
    topic_suffix = "__".join(
        _sanitize_consumer_group_part(topic) for topic in normalized_topics
    )
    if not topic_suffix:
        return CONSUMER_GROUP_ID
    return f"{CONSUMER_GROUP_ID}.{topic_suffix}"


class TooManyFailedAttemptsError(Exception):
    """Exception raised when operations exceed the maximum number of retry attempts

    This exception is typically raised during Kafka topic creation or connection
    establishment when the maximum number of retry attempts has been exceeded.
    """

    pass


class KafkaMessageFetchException(Exception):
    """Exception raised when Kafka message consumption fails

    This exception is raised when there are errors during the process of fetching
    or consuming messages from Kafka topics, including network issues, timeout
    errors, or malformed message data.
    """

    pass


class KafkaHandler:
    """Base class for all Kafka wrappers and handlers

    Provides common initialization and configuration setup for Kafka producers
    and consumers. This abstract base class establishes the foundation for
    specific Kafka handling implementations.
    """

    def __init__(self) -> None:
        """
        Sets up the initial configuration and initializes the consumer attribute
        to None. Specific implementations should override this method to establish
        their respective Kafka clients.
        """
        self.consumer = None


class KafkaProduceHandler(KafkaHandler):
    """Abstract base class for Kafka Producer wrappers

    Extends KafkaHandler to provide producer-specific functionality. This class
    establishes the interface for Kafka message production with different
    semantic guarantees (simple vs exactly-once).
    """

    def __init__(self, conf):
        """
        Args:
            conf (dict): Configuration dictionary for the Kafka producer.
                         Should contain broker settings and producer-specific options.
        """
        super().__init__()
        self.conf = conf
        self.producer = self._new_producer()

    def _new_producer(self):
        return retry_forever(
            lambda: Producer(self.conf),
            "Kafka producer creation",
        )

    def _reset_producer(self) -> None:
        try:
            if self.producer:
                self.producer.flush(5)
        except Exception as exception:
            logger.warning(
                "Ignoring Kafka producer flush failure during reconnect: %s", exception
            )
        self.producer = self._new_producer()

    def _with_producer_retry(
        self, description: str, operation: Callable[[], None]
    ) -> None:
        def attempt():
            try:
                operation()
            except Exception as exception:
                if not _is_retriable_kafka_exception(exception):
                    raise
                logger.warning(
                    "%s failed, recreating Kafka producer: %s", description, exception
                )
                self._reset_producer()
                raise

        retry_forever(
            attempt,
            description,
            retryable=(KafkaException, BufferError, RuntimeError, OSError),
        )

    @abstractmethod
    def produce(self, *args, **kwargs):
        """Abstract method for producing messages to Kafka topics

        Encodes the given data for transport and sends it to the specified topic.
        Implementations must define the specific behavior for message production.

        Args:
            *args: Variable arguments depending on implementation.
            **kwargs: Keyword arguments depending on implementation.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError

    def __del__(self) -> None:
        """Cleanup method called when the object is destroyed

        Ensures that all pending messages are flushed before the producer
        is destroyed, preventing message loss.
        """
        if self.producer:
            self.producer.flush()


class SimpleKafkaProduceHandler(KafkaProduceHandler):
    """Simple Kafka Producer wrapper without Write-Exactly-Once semantics

    Provides basic message production capabilities with at-least-once delivery
    guarantees. This implementation prioritizes simplicity and performance over
    strict consistency guarantees.
    """

    def __init__(self):
        """
        Sets up a Kafka producer with standard configuration for simple message
        production without transactional guarantees. Broker addresses are
        automatically configured from the global KAFKA_BROKERS setting.
        """
        self.brokers = ",".join(
            [
                f"{broker['hostname']}:{broker['internal_port']}"
                for broker in KAFKA_BROKERS
            ]
        )

        conf = {
            "bootstrap.servers": self.brokers,
            "enable.idempotence": False,
            "acks": "1",
            "message.max.bytes": 1000000000,
        }

        super().__init__(conf)

    def produce(self, topic: str, data: str, key: None | str = None) -> None:
        """Produce a message to the specified Kafka topic.

        Encodes and sends the provided data to the specified topic. The producer
        is flushed before sending to ensure message delivery. Empty data is
        silently ignored.

        Args:
            topic (str): Target Kafka topic name.
            data (str): Message data to send (ignored if empty).
            key (str, optional): Optional message key for partitioning.
                                 Default: None.

        Raises:
            KafkaException: If message production fails.
            BufferError: If the producer's message buffer is full.
        """
        if not data:
            return

        def operation():
            delivery_errors = []

            def delivery_callback(err, msg):
                kafka_delivery_report(err, msg)
                if err:
                    delivery_errors.append(err)

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
                if _is_retriable_kafka_error(delivery_error):
                    raise KafkaException(delivery_error)
                raise ValueError(f"Kafka delivery failed: {delivery_error}")

        self._with_producer_retry(f"Kafka produce to {topic}", operation)


class BufferedKafkaProduceHandler(SimpleKafkaProduceHandler):
    """An asynchronous Kafka producer with bounded local backpressure.

    This producer is intended for high-volume, non-critical telemetry such as
    the monitoring events eventually written to ClickHouse.  Unlike
    :class:`SimpleKafkaProduceHandler`, it does not wait for a broker
    acknowledgement after every record.  librdkafka batches records in the
    background and delivery callbacks are served by ``poll(0)`` calls made
    while more telemetry is being queued.

    If Kafka cannot keep up, the bounded local queue eventually fills.  At that
    point ``produce`` waits for delivery reports instead of allocating
    unbounded memory or discarding monitoring records.
    """

    _QUEUE_POLL_TIMEOUT_SECONDS = 0.1

    def __init__(self):
        """Create a batched producer with a bounded in-memory queue."""
        self.brokers = ",".join(
            [
                f"{broker['hostname']}:{broker['internal_port']}"
                for broker in KAFKA_BROKERS
            ]
        )
        conf = {
            "bootstrap.servers": self.brokers,
            "enable.idempotence": False,
            "acks": "1",
            "message.max.bytes": 1000000000,
            "linger.ms": 10,
            "batch.num.messages": 1000,
            "queue.buffering.max.messages": 10000,
        }
        KafkaProduceHandler.__init__(self, conf)

    def produce(self, topic: str, data: str, key: None | str = None) -> None:
        """Queue telemetry for delivery without flushing the producer.

        The producer queue is deliberately bounded.  ``BufferError`` is normal
        backpressure, so wait for delivery reports and retry it without
        recreating the producer.  Other transport failures retain the existing
        reconnect-and-retry behavior.
        """
        if not data:
            return

        def delivery_callback(err, msg):
            kafka_delivery_report(err, msg)

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
    """Kafka Producer wrapper with Write-Exactly-Once semantics

    Provides transactional message production with exactly-once delivery
    guarantees. This implementation ensures that messages are delivered
    exactly once, even in the presence of failures and retries.

    Configuration:
        - transactional.id: Set to HOSTNAME for unique transaction identification
        - enable.idempotence: True (required for exactly-once semantics)

    Note:
        Each instance must have a unique transactional.id to avoid conflicts.
    """

    def __init__(self):
        """
        Sets up a Kafka producer with transactional capabilities for exactly-once
        semantics. The producer is initialized with transactions enabled and
        configured with a unique transactional ID based on the hostname.

        Raises:
            KafkaException: If transaction initialization fails.
        """
        self.brokers = ",".join(
            [
                f"{broker['hostname']}:{broker['internal_port']}"
                for broker in KAFKA_BROKERS
            ]
        )

        conf = {
            "bootstrap.servers": self.brokers,
            "transactional.id": f"{HOSTNAME}-{uuid.uuid4()}",
            "enable.idempotence": True,
            "message.max.bytes": 1000000000,
        }

        super().__init__(conf)
        self._init_transactions_with_retry()

    def _reset_producer(self) -> None:
        super()._reset_producer()
        self._init_transactions_with_retry()

    def _init_transactions_with_retry(self) -> None:
        retry_forever(
            lambda: self.producer.init_transactions(),
            "Kafka transactional producer initialization",
        )

    def produce(self, topic: str, data: str, key: None | str = None) -> None:
        """Produce a message to the specified Kafka topic with exactly-once semantics.

        Sends the provided data within a Kafka transaction to ensure exactly-once
        delivery. The transaction is automatically committed on success or aborted
        on failure. Empty data is silently ignored.

        Args:
            topic (str): Target Kafka topic name.
            data (str): Message data to send (ignored if empty).
            key (str, optional): Optional message key for partitioning.
                                 Default: None.

        Raises:
            KafkaException: If message production or transaction handling fails.
            RuntimeError: If transaction commit fails after retries.
        """
        if not data:
            return

        def operation():
            self.producer.flush()
            self.producer.begin_transaction()

            try:
                self.producer.produce(
                    topic=topic,
                    key=key,
                    value=data,
                    callback=kafka_delivery_report,
                )
                self.commit_transaction_with_retry()
            except Exception as e:
                logger.info(f"aborted for topic {topic}")
                try:
                    self.producer.abort_transaction()
                except Exception as abort_exception:
                    logger.warning(
                        "Kafka transaction abort failed: %s", abort_exception
                    )
                logger.error("Transaction aborted.")
                logger.error(e)
                raise

        self._with_producer_retry(f"Kafka transactional produce to {topic}", operation)

    def commit_transaction_with_retry(
        self, max_retries: int = 3, retry_interval_ms: int = 1000
    ) -> None:
        """Commit a Kafka transaction with automatic retry logic.

        Attempts to commit the current transaction with built-in retry mechanism
        for handling transient failures. If committing fails due to conflicting
        API calls, the method will retry after the specified interval.

        Args:
            max_retries (int): Maximum number of commit retry attempts. Default: 3.
            retry_interval_ms (int): Time to wait between retries in milliseconds.
                                     Default: 1000.

        Raises:
            KafkaException: If transaction commit fails for reasons other than
                            conflicting API calls.
            RuntimeError: If transaction commit fails after all retry attempts.
        """
        committed = False
        retry_count = 0

        while not committed and retry_count < max_retries:
            try:
                self.producer.commit_transaction()
                committed = True
            except KafkaException as e:
                if (
                    "Conflicting commit_transaction API call is already in progress"
                    in str(e)
                ):
                    retry_count += 1
                    logger.debug(
                        "Conflicting commit_transaction API call is already in progress: Retrying"
                    )
                    time.sleep(retry_interval_ms / 1000.0)
                else:
                    raise e

        if not committed:
            raise RuntimeError("Failed to commit transaction after retries.")


class KafkaConsumeHandler(KafkaHandler):
    """Abstract base class for Kafka Consumer wrappers

    Provides common functionality for Kafka message consumption including
    topic creation, subscription management, and consumer configuration.
    All consumer implementations should extend this class.

    Attributes:
        brokers (str): Comma-separated list of Kafka broker addresses.
        consumer (Consumer): Confluent Kafka Consumer instance.
    """

    def __init__(self, topics: str | list[str]) -> None:
        """
        Creates a Kafka consumer, ensures the specified topics exist, and
        subscribes to them. Topics are automatically created if they don't exist.

        Args:
            topics (str | list[str]): Topic name(s) to subscribe to.
                                      Can be a single topic string or list of topics.

        Raises:
            TooManyFailedAttemptsError: If topic creation fails after retries.
            KafkaException: If consumer creation or subscription fails.
        """
        super().__init__()
        self._last_consumed_message = None

        if isinstance(topics, str):
            topics = [topics]
        self.topics = topics

        # get brokers
        self.brokers = ",".join(
            [
                f"{broker['hostname']}:{broker['internal_port']}"
                for broker in KAFKA_BROKERS
            ]
        )
        self.conf = self._build_consumer_conf()
        self._connect_consumer()

    def _build_consumer_conf(self) -> dict:
        return {
            "bootstrap.servers": self.brokers,
            "group.id": build_consumer_group_id(self.topics),
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
            "max.poll.interval.ms": KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS,
        }

    def _connect_consumer(self) -> None:
        def connect():
            consumer = Consumer(self.conf)
            admin_client = AdminClient(
                {
                    "bootstrap.servers": self.brokers,
                }
            )
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
            connect, f"Kafka consumer setup for {self.topics}"
        )

    def _reset_consumer(self) -> None:
        try:
            if self.consumer:
                self.consumer.close()
        except Exception as exception:
            logger.warning(
                "Ignoring Kafka consumer close failure during reconnect: %s", exception
            )
        self._last_consumed_message = None
        self._connect_consumer()

    def commit(self) -> None:
        """Commit the last message returned by ``consume``."""
        if self.consumer and self._last_consumed_message is not None:
            retry_forever(
                lambda: self.consumer.commit(self._last_consumed_message),
                "Kafka consumer offset commit",
                retryable=(KafkaException, RuntimeError, OSError),
            )
            self._last_consumed_message = None

    @abstractmethod
    def consume(self, *args, **kwargs):
        """Abstract method for consuming messages from Kafka topics

        Implementations must define the specific behavior for message consumption,
        including how to handle message polling, error handling, and data decoding.

        Args:
            *args: Variable arguments depending on implementation.
            **kwargs: Keyword arguments depending on implementation.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError

    def consume_as_json(self) -> tuple[Optional[str], dict]:
        """Consume messages and return them in JSON format.

        Consumes available messages from subscribed topics, decodes the data,
        and returns the contents as a JSON dictionary. This method blocks
        until a message is available.

        Returns:
            tuple[Optional[str], dict]: A tuple containing:
                - Message key (str or None)
                - Message value as dictionary (empty dict if no message)

        Raises:
            ValueError: If the message data format is invalid or cannot be parsed.
        """
        key, value, topic = self.consume()

        if not key and not value:
            return None, {}

        try:
            eval_data = json.loads(value)

            if isinstance(eval_data, dict):
                return key, eval_data
            else:
                raise
        except Exception:
            raise ValueError("Unknown data format")

    def _all_topics_created(
        self,
        topics: list[str],
        min_partitions: int | dict[str, int] = 1,
        consumer=None,
    ) -> bool:
        """Verify that all specified topics have been created successfully.

        Polls the Kafka cluster to check if each topic in the provided list
        has been created. Retries for a maximum duration if topics are not
        immediately available.

        Args:
            topics (list[str]): List of topic names to verify.

        Returns:
            bool: True if all topics are created, False if timeout exceeded.
        """
        number_of_retries_left = 30
        all_topics_created = False
        consumer = consumer or self.consumer
        while not all_topics_created:  # try for 15 seconds
            assigned_topics = retry_forever(
                lambda: consumer.list_topics(timeout=10),
                "Kafka topic visibility check",
                retryable=(KafkaException, RuntimeError, OSError),
            )

            all_topics_created = True
            for topic in topics:
                partition_count = _topic_partition_count(assigned_topics, topic)
                required_partitions = (
                    min_partitions.get(topic, 1)
                    if isinstance(min_partitions, dict)
                    else min_partitions
                )
                if partition_count is None or partition_count < required_partitions:
                    all_topics_created = False

            if not all_topics_created:
                number_of_retries_left -= 1

            if not number_of_retries_left > 0:
                return False

            time.sleep(0.5)

        return True

    def _poll_message(self):
        while True:
            try:
                msg = self.consumer.poll(timeout=1.0)
            except (KafkaException, RuntimeError, OSError) as exception:
                logger.warning(
                    "Kafka consumer poll failed, reconnecting: %s", exception
                )
                self._reset_consumer()
                continue

            if msg is None:
                return None

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    return None
                if _is_retriable_kafka_error(msg.error()):
                    logger.warning(
                        "Kafka consumer error is retriable, reconnecting: %s",
                        msg.error(),
                    )
                    self._reset_consumer()
                    return None

            return msg

    def __del__(self) -> None:
        """Cleanup method called when the object is destroyed

        Properly closes the Kafka consumer connection to release resources
        and ensure graceful shutdown.
        """
        if self.consumer:
            self.consumer.close()

    @staticmethod
    def _is_dicts(obj):
        return isinstance(obj, list) and all(isinstance(item, dict) for item in obj)

    @staticmethod
    def _decode_batch_data(data):
        if data is None:
            return []
        if not isinstance(data, list):
            raise ValueError("Batch data must be a list.")

        decoded_data = []
        for item in data:
            if isinstance(item, str):
                decoded_data.append(json.loads(item))
            elif isinstance(item, (dict, list)):
                decoded_data.append(item)
            else:
                raise ValueError("Batch data contains unsupported item type.")
        return decoded_data

    def consume_as_object(self) -> tuple[None | str, Batch]:
        """
        Consumes available messages on the specified topic. Decodes the data and converts it to a Batch
        object. Returns the Batch object.

        Returns:
            Consumed data as Batch object

        Raises:
            ValueError: Invalid data format
        """
        key, value, topic = self.consume()
        if not key and not value:
            # TODO: Change return value to fit the type, maybe switch to raise
            return None, {}
        eval_data: dict = json.loads(value)
        eval_data["data"] = self._decode_batch_data(eval_data.get("data"))
        batch_schema = marshmallow_dataclass.class_schema(Batch)()
        eval_data: Batch = batch_schema.load(eval_data)
        if isinstance(eval_data, Batch):
            return key, eval_data
        else:
            raise ValueError("Unknown data format.")


class SimpleKafkaConsumeHandler(KafkaConsumeHandler):
    """Simple Kafka Consumer wrapper without Write-Exactly-Once semantics

    Provides basic message consumption capabilities with at-least-once delivery
    semantics. Messages are not automatically committed, allowing for manual
    offset management by the application.
    """

    def __init__(self, topics: str | list[str]) -> None:
        """
        Args:
            topics (str | list[str]): Topic name(s) to subscribe to.
        """
        super().__init__(topics)

    def consume(self) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Consume messages from subscribed Kafka topics.

        Polls for available messages and decodes them. This method blocks
        until a message is available or a keyboard interrupt is received.
        The consumer does not automatically commit offsets.

        Returns:
            tuple[Optional[str], Optional[str], Optional[str]]: A tuple containing:
                - Message key (str or None)
                - Message value (str or None)
                - Topic name (str or None)

            Returns (None, None, None) if no valid message is retrieved.

        Raises:
            ValueError: If the received message is invalid.
            KeyboardInterrupt: If consumption is interrupted by user.
            KafkaException: If message commit fails.
        """
        empty_data_retrieved = False

        try:
            while True:
                msg = self._poll_message()

                if msg is None:
                    if not empty_data_retrieved:
                        logger.info("Waiting for messages...")

                    empty_data_retrieved = True
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    raise ValueError("Message is invalid")

                # unpack message
                key = msg.key().decode("utf-8") if msg.key() else None
                value = msg.value().decode("utf-8") if msg.value() else None
                topic = msg.topic() if msg.topic() else None
                self._last_consumed_message = msg
                return key, value, topic
        except KeyboardInterrupt:
            logger.info("Stopping KafkaConsumeHandler...")


class ExactlyOnceKafkaConsumeHandler(KafkaConsumeHandler):
    """Kafka Consumer wrapper with Write-Exactly-Once semantics

    Provides message consumption with exactly-once processing guarantees.
    Messages are automatically committed after successful processing to
    ensure each message is processed exactly once.
    """

    def __init__(self, topics: str | list[str]) -> None:
        """
        Args:
            topics (str | list[str]): Topic name(s) to subscribe to.
        """
        super().__init__(topics)

    def consume(self) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Consume messages from subscribed Kafka topics with exactly-once semantics.

        Polls for available messages, decodes them, and automatically commits
        the message offset after successful processing. This ensures each
        message is processed exactly once.

        Returns:
            tuple[Optional[str], Optional[str], Optional[str]]: A tuple containing:
                - Message key (str or None)
                - Message value (str or None)
                - Topic name (str or None)

            Returns (None, None, None) if no valid message is retrieved.

        Raises:
            ValueError: If the received message is invalid.
            KeyboardInterrupt: If consumption is interrupted by user.
            KafkaException: If message commit fails.
        """
        empty_data_retrieved = False

        try:
            while True:
                msg = self._poll_message()

                if msg is None:
                    if not empty_data_retrieved:
                        logger.info("Waiting for messages...")

                    empty_data_retrieved = True
                    continue

                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    raise ValueError("Message is invalid")

                # unpack message
                key = msg.key().decode("utf-8") if msg.key() else None
                value = msg.value().decode("utf-8") if msg.value() else None
                topic = msg.topic() if msg.topic() else None
                self._last_consumed_message = msg

                return key, value, topic
        except KeyboardInterrupt:
            logger.info("Shutting down KafkaConsumeHandler...")

    # @staticmethod
    # def _is_dicts(obj):
    #     """Check if the provided object is a list containing only dictionaries.

    #     Args:
    #         obj: Object to check.

    #     Returns:
    #         bool: True if obj is a list of dictionaries, False otherwise.
    #     """
    #     return isinstance(obj, list) and all(isinstance(item, dict) for item in obj)

    # def consume_as_object(self) -> tuple[Optional[str], Batch]:
    #     """
    #     Consume messages and return them as Batch objects.

    #     Consumes available messages from subscribed topics, decodes the data,
    #     and converts it to a structured Batch object using marshmallow schema
    #     validation. This method provides type-safe message consumption.

    #     Returns:
    #         tuple[Optional[str], Batch]: A tuple containing:
    #             - Message key (str or None).
    #             - Batch object containing the deserialized message data.

    #     Raises:
    #         ValueError: If the message data format is invalid or cannot be
    #                     converted to a Batch object.
    #         marshmallow.ValidationError: If data doesn't conform to Batch schema.
    #     """
    #     key, value, topic = self.consume()

    #     if not key and not value:
    #         # TODO: Change return value to fit the type, maybe switch to raise
    #         return None, {}

    #     eval_data: dict = ast.literal_eval(value)

    #     if self._is_dicts(eval_data.get("data")):
    #         eval_data["data"] = eval_data.get("data")
    #     else:
    #         eval_data["data"] = [
    #             ast.literal_eval(item) for item in eval_data.get("data")
    #         ]

    #     batch_schema = marshmallow_dataclass.class_schema(Batch)()
    #     eval_data: Batch = batch_schema.load(eval_data)

    #     if isinstance(eval_data, Batch):
    #         return key, eval_data
    #     else:
    #         raise ValueError("Unknown data format.")

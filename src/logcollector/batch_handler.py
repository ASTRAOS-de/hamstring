import datetime
import json
import uuid

from src.base.clickhouse_kafka_sender import ClickHouseKafkaSender
from src.base.utils import setup_config
from src.base.log_config import get_logger

module_name = "log_collection.batch_handler"
logger = get_logger(module_name)

config = setup_config()


class BufferedBatch:
    """Data structure for managing batches, buffers, and timestamps in the log collection pipeline

    Manages a current batch and the previous batch retained as overlap for each
    routing key. Scheduling and Kafka publication belong to ``LogCollector``.
    """

    def __init__(
        self,
        collector_name,
        monitoring_kafka_producer=None,
    ):
        self.name = f"buffered-batch-for-{collector_name}"
        self.batch = {}  # Batch for the latest messages coming in
        self.buffer = {}  # Former batch with previous messages
        self.batch_id = {}  # Batch ID per key
        self.monitoring_kafka_producer = (
            monitoring_kafka_producer or ClickHouseKafkaSender.create_shared_producer()
        )

        # databases
        self.logline_to_batches = ClickHouseKafkaSender(
            "logline_to_batches", self.monitoring_kafka_producer
        )
        self.batch_timestamps = ClickHouseKafkaSender(
            "batch_timestamps", self.monitoring_kafka_producer
        )
        self.fill_levels = ClickHouseKafkaSender(
            "fill_levels", self.monitoring_kafka_producer
        )
        self.batch_tree = ClickHouseKafkaSender(
            "batch_tree", self.monitoring_kafka_producer
        )
        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
                stage=module_name,
                entry_type="total_loglines_in_batches",
                entry_count=0,
            )
        )

        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
                stage=module_name,
                entry_type="total_loglines_in_buffer",
                entry_count=0,
            )
        )

    def add_message(self, key: str, logline_id: uuid.UUID, message: str) -> None:
        """Adds a message to the batch associated with the given key.

        If the key does not exist in the current batch, a new batch entry is created with a unique batch ID.
        For existing keys, the message is appended to the existing batch. Logs the association between the
        logline and batch ID, updates batch timestamps, and tracks fill levels for monitoring purposes.

        Args:
            key (str): Key to which the message is added (typically subnet ID).
            logline_id (uuid.UUID): Unique identifier of the logline message.
            message (str): JSON-formatted message to be added to the batch.
        """
        if key in self.batch:  # key already has messages associated
            self.batch[key].append(message)

            batch_id = self.batch_id.get(key)
            self.logline_to_batches.insert(
                dict(
                    timestamp=datetime.datetime.now(),
                    logline_id=logline_id,
                    batch_id=batch_id,
                )
            )

            self.batch_timestamps.insert(
                dict(
                    batch_id=batch_id,
                    stage=module_name,
                    instance_name=self.name,
                    status="waiting",
                    timestamp=datetime.datetime.now(),
                    is_active=True,
                    message_count=self.get_message_count_for_batch_key(key),
                )
            )

        else:  # key has no messages associated yet
            # create new batch
            self.batch[key] = [message]
            new_batch_id = uuid.uuid4()
            self.batch_id[key] = new_batch_id

            self.logline_to_batches.insert(
                dict(
                    timestamp=datetime.datetime.now(),
                    logline_id=logline_id,
                    batch_id=new_batch_id,
                )
            )

            self.batch_timestamps.insert(
                dict(
                    batch_id=new_batch_id,
                    stage=module_name,
                    instance_name=self.name,
                    status="waiting",
                    timestamp=datetime.datetime.now(),
                    is_active=True,
                    message_count=1,
                )
            )

        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
                stage=module_name,
                entry_type="total_loglines_in_batches",
                entry_count=self.get_message_count_for_batch(),
            )
        )

    def get_message_count_for_batch(self) -> int:
        """Returns the total number of messages across all batches.

        Calculates the sum of message counts from all key-specific batches currently stored.

        Returns:
            Total number of messages in all batches.
        """
        return sum(len(key_entry) for key_entry in self.batch.values())

    def get_message_count_for_buffer(self) -> int:
        """Returns the total number of messages across all buffers.

        Calculates the sum of message counts from all key-specific buffers currently stored.

        Returns:
            Total number of messages in all buffers.
        """
        return sum(len(key_entry) for key_entry in self.buffer.values())

    def get_message_count_for_batch_key(self, key: str) -> int:
        """Returns the number of messages in the batch for a specific key.

        Args:
            key (str): Key for which message count is returned.

        Returns:
            Number of messages in the batch for the given key, or 0 if key doesn't exist.
        """
        if key in self.batch:
            return len(self.batch[key])

        return 0

    def get_message_count_for_buffer_key(self, key: str) -> int:
        """Returns the number of messages in the buffer for a specific key.

        Args:
            key (str): Key for which message count is returned.

        Returns:
            Number of messages in the buffer for the given key, or 0 if key doesn't exist.
        """
        if key in self.buffer:
            return len(self.buffer[key])

        return 0

    def complete_batch(self, key: str) -> dict | None:
        """Complete one current batch or expire its previous overlap."""
        current_messages = self.batch.get(key)
        if not current_messages:
            if self.buffer.pop(key, None) is not None:
                self._record_fill_level(
                    "total_loglines_in_buffer", self.get_message_count_for_buffer()
                )
            return None

        current_messages = self._sorted_messages(current_messages)
        previous_messages = self._sorted_messages(self.buffer.get(key, []))
        batch_id = self.batch_id[key]
        row_id = str(uuid.uuid4())
        first_message = (
            previous_messages[0] if previous_messages else current_messages[0]
        )
        data = {
            "batch_tree_row_id": row_id,
            "batch_id": batch_id,
            "begin_timestamp": datetime.datetime.fromisoformat(
                json.loads(first_message)["ts"]
            ),
            "end_timestamp": datetime.datetime.fromisoformat(
                json.loads(current_messages[-1])["ts"]
            ),
            "data": previous_messages + current_messages,
        }

        timestamp = datetime.datetime.now()
        self.batch_timestamps.insert(
            dict(
                batch_id=batch_id,
                stage=module_name,
                instance_name=self.name,
                status="completed",
                timestamp=timestamp,
                is_active=True,
                message_count=len(current_messages),
            )
        )
        self.batch_tree.insert(
            dict(
                batch_row_id=row_id,
                parent_batch_row_id=None,
                stage=module_name,
                instance_name=self.name,
                timestamp=timestamp,
                status="completed",
                batch_id=batch_id,
            )
        )

        self.buffer[key] = current_messages
        del self.batch[key]
        del self.batch_id[key]
        self._record_fill_level(
            "total_loglines_in_batches", self.get_message_count_for_batch()
        )
        self._record_fill_level(
            "total_loglines_in_buffer", self.get_message_count_for_buffer()
        )
        return data

    def get_stored_keys(self) -> set:
        """Retrieves all keys stored in either the batch or the buffer.

        Combines keys from both the current batch dictionary and the buffer dictionary
        to provide a complete set of all keys that have associated data.

        Returns:
            Set of all unique keys stored in either batch or buffer dictionaries.
        """
        return set(self.batch) | set(self.buffer)

    @staticmethod
    def _sorted_messages(data: list[str]) -> list[str]:
        return sorted(data, key=lambda message: str(json.loads(message).get("ts", "")))

    def _record_fill_level(self, entry_type: str, entry_count: int) -> None:
        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
                stage=module_name,
                entry_type=entry_type,
                entry_count=entry_count,
            )
        )


class BatchAccumulator:
    """Collect subnet batches without owning scheduling or Kafka production."""

    def __init__(
        self,
        collector_name,
        monitoring_kafka_producer=None,
    ):
        self.monitoring_kafka_producer = (
            monitoring_kafka_producer or ClickHouseKafkaSender.create_shared_producer()
        )
        self.batch = BufferedBatch(collector_name, self.monitoring_kafka_producer)

        # databases
        self.logline_timestamps = ClickHouseKafkaSender(
            "logline_timestamps", self.monitoring_kafka_producer
        )

    def add_message(self, key: str, message: str) -> int:
        """Add one message and return the accumulated count for its key."""
        logline_id = json.loads(message).get("logline_id")
        self.logline_timestamps.insert(
            dict(
                logline_id=logline_id,
                stage=module_name,
                status="in_process",
                timestamp=datetime.datetime.now(),
                is_active=True,
            )
        )

        self.batch.add_message(key, logline_id, message)
        self.logline_timestamps.insert(
            dict(
                logline_id=logline_id,
                stage=module_name,
                status="batched",
                timestamp=datetime.datetime.now(),
                is_active=True,
            )
        )

        logger.debug("Batch: %s", self.batch.batch)
        return self.batch.get_message_count_for_batch_key(key)

    def complete_all(self) -> list[tuple[str, dict]]:
        """Complete and remove every currently buffered key."""
        packets = []
        for key in self.batch.get_stored_keys():
            packet = self.batch.complete_batch(key)
            if packet is not None:
                packets.append((key, packet))
        return packets

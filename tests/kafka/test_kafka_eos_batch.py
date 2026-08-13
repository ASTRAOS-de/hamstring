import unittest
from unittest.mock import MagicMock, call, patch

from confluent_kafka import KafkaError, KafkaException

from src.base.kafka import (
    ConsumedKafkaMessage,
    ExactlyOnceKafkaConsumeHandler,
    ExactlyOnceKafkaProduceHandler,
    KafkaConsumerMembershipLost,
)


class TestTransactionalBatch(unittest.TestCase):
    @patch("src.base.kafka.producer.Producer")
    def test_outputs_and_source_offsets_are_committed_in_one_transaction(
        self, mock_producer
    ):
        producer = mock_producer.return_value
        consumer = MagicMock()
        offsets = [MagicMock()]
        group_metadata = MagicMock()
        consumer.offsets_for.return_value = offsets
        consumer.group_metadata.return_value = group_metadata
        source_messages = [
            ConsumedKafkaMessage("source-key", "source-value", "source", 2, 7)
        ]
        handler = ExactlyOnceKafkaProduceHandler()

        with handler.transaction_batch(consumer, source_messages):
            handler.produce("output", "first", key="routing-key")
            handler.produce("output", "second", key="routing-key")

        producer.begin_transaction.assert_called_once_with()
        self.assertEqual(
            [
                call(
                    topic="output",
                    key="routing-key",
                    value="first",
                    callback=unittest.mock.ANY,
                ),
                call(
                    topic="output",
                    key="routing-key",
                    value="second",
                    callback=unittest.mock.ANY,
                ),
            ],
            producer.produce.call_args_list,
        )
        producer.send_offsets_to_transaction.assert_called_once_with(
            offsets, group_metadata
        )
        producer.commit_transaction.assert_called_once_with()

    @patch("src.base.kafka.producer.Producer")
    def test_source_offsets_are_committed_when_processing_has_no_output(
        self, mock_producer
    ):
        producer = mock_producer.return_value
        consumer = MagicMock()
        source_messages = [
            ConsumedKafkaMessage("source-key", "source-value", "source", 0, 4)
        ]
        handler = ExactlyOnceKafkaProduceHandler()

        with handler.transaction_batch(consumer, source_messages):
            pass

        producer.begin_transaction.assert_called_once_with()
        producer.produce.assert_not_called()
        producer.send_offsets_to_transaction.assert_called_once_with(
            consumer.offsets_for.return_value,
            consumer.group_metadata.return_value,
        )
        producer.commit_transaction.assert_called_once_with()

    @patch(
        "src.base.kafka.producer.retry_forever",
        side_effect=lambda operation, *_args, **_kwargs: operation(),
    )
    @patch("src.base.kafka.producer.Producer")
    def test_stale_consumer_membership_aborts_and_recovers_without_producer_retry(
        self, mock_producer, _mock_retry_forever
    ):
        producer = mock_producer.return_value
        consumer = MagicMock()
        source_messages = [
            ConsumedKafkaMessage("source-key", "source-value", "source", 0, 4)
        ]
        producer.send_offsets_to_transaction.side_effect = KafkaException(
            KafkaError(KafkaError.UNKNOWN_MEMBER_ID)
        )
        handler = ExactlyOnceKafkaProduceHandler()

        with handler.transaction_batch(consumer, source_messages):
            handler.produce("output", "result", key="source-key")

        producer.abort_transaction.assert_called_once_with()
        producer.commit_transaction.assert_not_called()
        consumer.recover_group_membership.assert_called_once()
        self.assertEqual(1, mock_producer.call_count)

    @patch(
        "src.base.kafka.producer.retry_forever",
        side_effect=lambda operation, *_args, **_kwargs: operation(),
    )
    @patch("src.base.kafka.producer.Producer")
    def test_all_stale_group_errors_take_the_consumer_recovery_path(
        self, mock_producer, _mock_retry_forever
    ):
        membership_error_names = (
            "UNKNOWN_MEMBER_ID",
            "ILLEGAL_GENERATION",
            "REBALANCE_IN_PROGRESS",
        )

        for error_name in membership_error_names:
            with self.subTest(error_name=error_name):
                producer = MagicMock()
                mock_producer.return_value = producer
                producer.send_offsets_to_transaction.side_effect = KafkaException(
                    KafkaError(getattr(KafkaError, error_name))
                )
                consumer = MagicMock()
                handler = ExactlyOnceKafkaProduceHandler()

                with handler.transaction_batch(
                    consumer,
                    [ConsumedKafkaMessage(None, "value", "source", 0, 1)],
                ):
                    handler.produce("output", "result")

                producer.abort_transaction.assert_called_once_with()
                consumer.recover_group_membership.assert_called_once()

    @patch(
        "src.base.kafka.producer.retry_forever",
        side_effect=lambda operation, *_args, **_kwargs: operation(),
    )
    @patch("src.base.kafka.producer.Producer")
    def test_transaction_can_succeed_after_membership_recovery(
        self, mock_producer, _mock_retry_forever
    ):
        producer = mock_producer.return_value
        producer.send_offsets_to_transaction.side_effect = [
            KafkaException(KafkaError(KafkaError.UNKNOWN_MEMBER_ID)),
            None,
        ]
        consumer = MagicMock()
        handler = ExactlyOnceKafkaProduceHandler()
        source_message = ConsumedKafkaMessage(None, "value", "source", 0, 1)

        with handler.transaction_batch(consumer, [source_message]):
            handler.produce("output", "first-attempt")

        with handler.transaction_batch(consumer, [source_message]):
            handler.produce("output", "replayed-attempt")

        consumer.recover_group_membership.assert_called_once()
        producer.abort_transaction.assert_called_once_with()
        producer.commit_transaction.assert_called_once_with()
        self.assertEqual(2, producer.begin_transaction.call_count)

    @patch("src.base.kafka.producer.Producer")
    def test_revoked_batch_is_rejected_before_a_transaction_begins(
        self, mock_producer
    ):
        producer = mock_producer.return_value
        consumer = MagicMock()
        consumer.ensure_batch_assignment_current.side_effect = (
            KafkaConsumerMembershipLost("assignment revoked")
        )
        handler = ExactlyOnceKafkaProduceHandler()

        with handler.transaction_batch(
            consumer,
            [ConsumedKafkaMessage(None, "value", "source", 0, 1)],
        ):
            handler.produce("output", "must-not-be-committed")

        producer.begin_transaction.assert_not_called()
        producer.produce.assert_not_called()
        consumer.recover_group_membership.assert_called_once()


class TestBatchConsumption(unittest.TestCase):
    def setUp(self):
        self.handler = object.__new__(ExactlyOnceKafkaConsumeHandler)
        self.handler.consumer = MagicMock()
        self.handler._last_consumed_message = None
        self.handler._assignment_epoch = 7
        self.handler._assignment_valid = True
        self.handler._assigned_partitions = {("source", 0), ("source", 1)}

    @staticmethod
    def _message(topic, partition, offset, key, value):
        message = MagicMock()
        message.error.return_value = None
        message.topic.return_value = topic
        message.partition.return_value = partition
        message.offset.return_value = offset
        message.key.return_value = key.encode()
        message.value.return_value = value.encode()
        return message

    def test_consume_batch_fetches_multiple_records_with_one_consumer_call(self):
        first = self._message("source", 0, 3, "key-1", "value-1")
        second = self._message("source", 1, 8, "key-2", "value-2")
        self.handler.consumer.consume.return_value = [first, second]

        records = self.handler.consume_batch(max_messages=2, timeout_ms=10)

        self.handler.consumer.consume.assert_called_once()
        self.assertEqual(
            [
                ("key-1", "value-1", "source", 0, 3),
                ("key-2", "value-2", "source", 1, 8),
            ],
            [
                (
                    record.key,
                    record.value,
                    record.topic,
                    record.partition,
                    record.offset,
                )
                for record in records
            ],
        )
        self.assertEqual({7}, {record.assignment_epoch for record in records})

    def test_offsets_use_next_offset_and_highest_record_per_partition(self):
        records = [
            ConsumedKafkaMessage(None, "one", "source", 0, 3),
            ConsumedKafkaMessage(None, "two", "source", 1, 8),
            ConsumedKafkaMessage(None, "three", "source", 0, 7),
        ]

        offsets = self.handler.offsets_for(records)

        self.assertEqual(
            {("source", 0, 8), ("source", 1, 9)},
            {(item.topic, item.partition, item.offset) for item in offsets},
        )

    def test_commit_batch_commits_all_partition_offsets_synchronously(self):
        records = [
            ConsumedKafkaMessage(None, "one", "source", 0, 3),
            ConsumedKafkaMessage(None, "two", "source", 1, 8),
            ConsumedKafkaMessage(None, "three", "source", 0, 7),
        ]

        self.handler.commit(records)

        self.handler.consumer.commit.assert_called_once()
        commit_kwargs = self.handler.consumer.commit.call_args.kwargs
        self.assertFalse(commit_kwargs["asynchronous"])
        self.assertEqual(
            {("source", 0, 8), ("source", 1, 9)},
            {
                (item.topic, item.partition, item.offset)
                for item in commit_kwargs["offsets"]
            },
        )

    def test_max_poll_error_resets_membership_and_returns_no_records(self):
        message = MagicMock()
        message.error.return_value = KafkaError(KafkaError._MAX_POLL_EXCEEDED)
        self.handler.consumer.consume.return_value = [message]
        self.handler.recover_group_membership = MagicMock()

        records = self.handler.consume_batch(max_messages=1, timeout_ms=10)

        self.assertEqual([], records)
        self.handler.recover_group_membership.assert_called_once()

    def test_revoke_invalidates_records_from_the_previous_assignment(self):
        record = ConsumedKafkaMessage(
            None,
            "value",
            "source",
            0,
            1,
            assignment_epoch=self.handler._assignment_epoch,
        )

        self.handler._on_revoke(
            self.handler.consumer,
            [MagicMock(topic="source", partition=0)],
        )

        with self.assertRaises(KafkaConsumerMembershipLost):
            self.handler.ensure_batch_assignment_current([record])

    def test_lost_assignment_invalidates_records(self):
        record = ConsumedKafkaMessage(
            None,
            "value",
            "source",
            1,
            2,
            assignment_epoch=self.handler._assignment_epoch,
        )

        self.handler._on_lost(
            self.handler.consumer,
            [MagicMock(topic="source", partition=1)],
        )

        with self.assertRaises(KafkaConsumerMembershipLost):
            self.handler.ensure_batch_assignment_current([record])

    def test_new_assignment_accepts_only_records_from_its_epoch(self):
        old_epoch = self.handler._assignment_epoch
        partitions = [MagicMock(topic="source", partition=0)]

        self.handler._on_assign(self.handler.consumer, partitions)

        current_record = ConsumedKafkaMessage(
            None,
            "current",
            "source",
            0,
            3,
            assignment_epoch=self.handler._assignment_epoch,
        )
        stale_record = ConsumedKafkaMessage(
            None,
            "stale",
            "source",
            0,
            2,
            assignment_epoch=old_epoch,
        )
        self.handler.ensure_batch_assignment_current([current_record])
        with self.assertRaises(KafkaConsumerMembershipLost):
            self.handler.ensure_batch_assignment_current([stale_record])


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import MagicMock, call, patch

from src.base.kafka import (
    ConsumedKafkaMessage,
    ExactlyOnceKafkaProduceHandler,
    KafkaProduceRecord,
)


class TestExactlyOnceKafkaProduceHandler(unittest.TestCase):
    @patch("src.base.kafka.producer.Producer")
    def test_transactional_id_is_required(self, producer_type):
        with self.assertRaisesRegex(ValueError, "transactional ID"):
            ExactlyOnceKafkaProduceHandler("")
        producer_type.assert_not_called()

    @patch("src.base.kafka.producer.Producer")
    def test_initializes_transactional_producer(self, producer_type):
        handler = ExactlyOnceKafkaProduceHandler("worker-1")

        configuration = producer_type.call_args.args[0]
        self.assertEqual("worker-1", configuration["transactional.id"])
        self.assertTrue(configuration["enable.idempotence"])
        producer_type.return_value.init_transactions.assert_called_once()
        self.assertIs(handler.producer, producer_type.return_value)

    @patch("src.base.kafka.producer.Producer")
    def test_publish_commits_all_records_in_one_transaction(self, producer_type):
        handler = ExactlyOnceKafkaProduceHandler("worker-1")
        producer = producer_type.return_value
        producer.reset_mock()

        handler.publish(
            [
                KafkaProduceRecord("one", "first", key="routing-key"),
                KafkaProduceRecord("two", "second"),
            ]
        )

        producer.begin_transaction.assert_called_once_with()
        self.assertEqual(2, producer.produce.call_count)
        producer.commit_transaction.assert_called_once()
        producer.abort_transaction.assert_not_called()

    @patch("src.base.kafka.producer.Producer")
    def test_complete_commits_outputs_and_source_offsets_together(self, producer_type):
        handler = ExactlyOnceKafkaProduceHandler("worker-1")
        producer = producer_type.return_value
        producer.reset_mock()
        consumer = MagicMock()
        offsets = [MagicMock()]
        metadata = MagicMock()
        consumer.offsets_for.return_value = offsets
        consumer.group_metadata.return_value = metadata
        sources = [ConsumedKafkaMessage(None, "source", "input", 3, 11)]

        handler.complete([KafkaProduceRecord("output", "result")], consumer, sources)

        consumer.offsets_for.assert_called_once_with(sources)
        consumer.group_metadata.assert_called_once_with()
        producer.send_offsets_to_transaction.assert_called_once_with(offsets, metadata)
        producer.commit_transaction.assert_called_once()

    @patch("src.base.kafka.producer.Producer")
    def test_complete_can_atomically_acknowledge_filtered_output(self, producer_type):
        handler = ExactlyOnceKafkaProduceHandler("worker-1")
        producer = producer_type.return_value
        producer.reset_mock()
        consumer = MagicMock()

        handler.complete(
            [],
            consumer,
            [ConsumedKafkaMessage(None, "source", "input", 0, 2)],
        )

        producer.produce.assert_not_called()
        producer.send_offsets_to_transaction.assert_called_once()
        producer.commit_transaction.assert_called_once()

    @patch("src.base.kafka.producer.Producer")
    def test_transaction_failure_is_aborted_and_propagated(self, producer_type):
        handler = ExactlyOnceKafkaProduceHandler("worker-1")
        producer = producer_type.return_value
        producer.reset_mock()
        producer.commit_transaction.side_effect = ValueError("permanent")

        with self.assertRaisesRegex(ValueError, "permanent"):
            handler.publish([KafkaProduceRecord("output", "result")])

        producer.abort_transaction.assert_called_once()

    @patch("src.base.kafka.producer.Producer")
    def test_complete_requires_source_records(self, producer_type):
        handler = ExactlyOnceKafkaProduceHandler("worker-1")

        with self.assertRaisesRegex(ValueError, "source records"):
            handler.complete([], MagicMock(), [])


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import MagicMock, patch

from src.base.kafka import (
    BufferedKafkaProduceHandler,
    ConsumedKafkaMessage,
    KafkaProduceRecord,
    SimpleKafkaProduceHandler,
)


class TestSimpleKafkaProduceHandler(unittest.TestCase):
    @patch("src.base.kafka.producer.Producer")
    def test_configuration_is_idempotent_and_synchronous(self, producer_type):
        handler = SimpleKafkaProduceHandler()

        configuration = producer_type.call_args.args[0]
        self.assertTrue(configuration["enable.idempotence"])
        self.assertEqual("all", configuration["acks"])
        self.assertEqual(
            "900000" if isinstance(configuration["message.max.bytes"], str) else 900000,
            configuration["message.max.bytes"],
        )
        self.assertIs(handler.producer, producer_type.return_value)

    @patch("src.base.kafka.producer.Producer")
    def test_publish_queues_all_records_and_flushes_once(self, producer_type):
        producer_type.return_value.flush.return_value = 0
        handler = SimpleKafkaProduceHandler()
        records = [
            KafkaProduceRecord("output-a", "one", key="key"),
            KafkaProduceRecord("output-b", "two", headers=(("correlation", b"id"),)),
        ]

        handler.publish(records)

        self.assertEqual(2, producer_type.return_value.produce.call_count)
        first = producer_type.return_value.produce.call_args_list[0].kwargs
        second = producer_type.return_value.produce.call_args_list[1].kwargs
        self.assertEqual(
            ("output-a", "key", "one"), (first["topic"], first["key"], first["value"])
        )
        self.assertEqual([("correlation", b"id")], second["headers"])
        producer_type.return_value.flush.assert_called_once_with()

    @patch("src.base.kafka.producer.Producer")
    def test_complete_publishes_before_committing_sources(self, producer_type):
        producer_type.return_value.flush.return_value = 0
        handler = SimpleKafkaProduceHandler()
        consumer = MagicMock()
        source = ConsumedKafkaMessage(None, "source", "input", 2, 7)

        handler.complete([KafkaProduceRecord("output", "value")], consumer, [source])

        producer_type.return_value.flush.assert_called_once_with()
        consumer.commit.assert_called_once_with([source])

    @patch("src.base.retry.time.sleep")
    @patch("src.base.kafka.producer.Producer")
    def test_publish_retries_when_flush_leaves_records_undelivered(
        self, producer_type, sleep
    ):
        producer_type.return_value.flush.side_effect = [1, 0, 0]
        handler = SimpleKafkaProduceHandler()

        handler.publish([KafkaProduceRecord("output", "value")])

        self.assertEqual(2, producer_type.call_count)
        self.assertEqual(2, producer_type.return_value.produce.call_count)
        sleep.assert_called_once()

    @patch("src.base.kafka.producer.Producer")
    def test_complete_requires_sources(self, producer_type):
        handler = SimpleKafkaProduceHandler()

        with self.assertRaisesRegex(ValueError, "source records"):
            handler.complete([], MagicMock(), [])


class TestBufferedKafkaProduceHandler(unittest.TestCase):
    @patch("src.base.kafka.producer.Producer")
    def test_publish_is_non_flushing_and_polls_delivery_callbacks(self, producer_type):
        handler = BufferedKafkaProduceHandler()

        handler.publish([KafkaProduceRecord("telemetry", "value")])

        producer_type.return_value.poll.assert_called_with(0)
        producer_type.return_value.produce.assert_called_once()
        producer_type.return_value.flush.assert_not_called()

    @patch("src.base.kafka.producer.Producer")
    def test_buffered_producer_cannot_acknowledge_pipeline_input(self, producer_type):
        handler = BufferedKafkaProduceHandler()

        with self.assertRaisesRegex(TypeError, "cannot commit"):
            handler.complete([], MagicMock(), [MagicMock()])


if __name__ == "__main__":
    unittest.main()

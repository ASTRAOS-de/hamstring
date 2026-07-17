import unittest
from unittest.mock import patch

from src.base.kafka import ConsumedKafkaMessage, SimpleKafkaConsumeHandler
from tests.kafka.test_kafka_consume_handler import kafka_message


class TestSimpleKafkaConsumeHandler(unittest.TestCase):
    @patch("src.base.kafka.consumer.KafkaTopicManager")
    @patch("src.base.kafka.consumer.AdminClient")
    @patch("src.base.kafka.consumer.Consumer")
    def test_consume_one_uses_common_record_contract(
        self, consumer_type, admin_type, ensure_topics
    ):
        consumer_type.return_value.consume.return_value = [kafka_message()]
        handler = SimpleKafkaConsumeHandler("input")

        self.assertEqual(
            ConsumedKafkaMessage("key", "value", "input", 2, 7),
            handler.consume_one(),
        )

    @patch("src.base.kafka.consumer.KafkaTopicManager")
    @patch("src.base.kafka.consumer.AdminClient")
    @patch("src.base.kafka.consumer.Consumer")
    def test_simple_mode_does_not_request_read_committed(
        self, consumer_type, admin_type, ensure_topics
    ):
        handler = SimpleKafkaConsumeHandler("input")

        self.assertNotIn("isolation.level", handler.conf)


if __name__ == "__main__":
    unittest.main()

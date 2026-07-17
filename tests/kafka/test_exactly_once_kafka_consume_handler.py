import unittest
from unittest.mock import patch

from src.base.kafka import ExactlyOnceKafkaConsumeHandler


class TestExactlyOnceKafkaConsumeHandler(unittest.TestCase):
    @patch("src.base.kafka.consumer.KafkaTopicManager")
    @patch("src.base.kafka.consumer.AdminClient")
    @patch("src.base.kafka.consumer.Consumer")
    def test_exactly_once_mode_reads_only_committed_records(
        self, consumer_type, admin_type, ensure_topics
    ):
        handler = ExactlyOnceKafkaConsumeHandler("input")

        self.assertEqual("read_committed", handler.conf["isolation.level"])
        consumer_type.assert_called_once_with(handler.conf)


if __name__ == "__main__":
    unittest.main()

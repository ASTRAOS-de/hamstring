import unittest
from unittest.mock import ANY, Mock, call, patch

from confluent_kafka import KafkaError

from src.base.kafka_handler import (
    BufferedKafkaProduceHandler,
    SimpleKafkaProduceHandler,
)


class TestInit(unittest.TestCase):
    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "internal_port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "internal_port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "internal_port": 9997,
            },
        ],
    )
    def test_successful(self):
        # Arrange
        expected_conf = {
            "bootstrap.servers": "127.0.0.1:9999,127.0.0.2:9998,127.0.0.3:9997",
            "enable.idempotence": False,
            "acks": "1",
            "message.max.bytes": 1000000000,
        }

        # Act
        with patch("src.base.kafka_handler.Producer") as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance

            sut = SimpleKafkaProduceHandler()

        # Assert
        self.assertEqual("127.0.0.1:9999,127.0.0.2:9998,127.0.0.3:9997", sut.brokers)
        self.assertIsNone(sut.consumer)
        mock_producer.assert_called_once_with(expected_conf)


class TestProduce(unittest.TestCase):
    def test_with_data(self):
        with patch("src.base.kafka_handler.Producer") as mock_producer:
            # Arrange
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance

            sut = SimpleKafkaProduceHandler()

            # Act
            sut.produce("test_topic", "test_data")

        # Assert
        self.assertEqual(2, mock_producer_instance.flush.call_count)
        mock_producer_instance.produce.assert_called_once_with(
            topic="test_topic",
            key=None,
            value="test_data",
            callback=ANY,
        )

    @patch("src.base.retry.time.sleep", return_value=None)
    def test_with_data_recreates_producer_after_transient_failure(self, mock_sleep):
        with patch("src.base.kafka_handler.Producer") as mock_producer:
            first_producer = Mock()
            second_producer = Mock()
            first_producer.flush.side_effect = BufferError("queue full")
            mock_producer.side_effect = [first_producer, second_producer]

            sut = SimpleKafkaProduceHandler()
            sut.produce("test_topic", "test_data")

        self.assertEqual(2, mock_producer.call_count)
        first_producer.flush.assert_called()
        second_producer.produce.assert_called_once_with(
            topic="test_topic",
            key=None,
            value="test_data",
            callback=ANY,
        )
        mock_sleep.assert_called()

    @patch("src.base.retry.time.sleep", return_value=None)
    def test_with_data_retries_delivery_callback_error(self, mock_sleep):
        with patch("src.base.kafka_handler.Producer") as mock_producer:
            first_producer = Mock()
            second_producer = Mock()
            delivery_error = KafkaError(KafkaError._ALL_BROKERS_DOWN)

            def fail_delivery(**kwargs):
                kwargs["callback"](delivery_error, None)

            first_producer.produce.side_effect = fail_delivery
            mock_producer.side_effect = [first_producer, second_producer]

            sut = SimpleKafkaProduceHandler()
            sut.produce("test_topic", "test_data")

        self.assertEqual(2, mock_producer.call_count)
        first_producer.produce.assert_called_once()
        second_producer.produce.assert_called_once_with(
            topic="test_topic",
            key=None,
            value="test_data",
            callback=ANY,
        )
        mock_sleep.assert_called()

    def test_without_data(self):
        with patch("src.base.kafka_handler.Producer") as mock_producer:
            # Arrange
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance

            sut = SimpleKafkaProduceHandler()

            # Act
            sut.produce("test_topic", "")

        # Assert
        mock_producer_instance.flush.assert_not_called()
        mock_producer_instance.produce.assert_not_called()


class TestBufferedProduce(unittest.TestCase):
    def test_queues_data_without_flushing(self):
        with patch("src.base.kafka_handler.Producer") as mock_producer:
            producer = Mock()
            mock_producer.return_value = producer
            sut = BufferedKafkaProduceHandler()

            sut.produce("test_topic", "test_data")

        producer.flush.assert_not_called()
        producer.poll.assert_called_once_with(0)
        producer.produce.assert_called_once_with(
            topic="test_topic",
            key=None,
            value="test_data",
            callback=ANY,
        )

    def test_waits_for_queue_space_without_recreating_producer(self):
        with patch("src.base.kafka_handler.Producer") as mock_producer:
            producer = Mock()
            producer.produce.side_effect = [BufferError("queue full"), None]
            mock_producer.return_value = producer
            sut = BufferedKafkaProduceHandler()

            sut.produce("test_topic", "test_data")

        mock_producer.assert_called_once()
        self.assertEqual(2, producer.produce.call_count)
        producer.poll.assert_has_calls([call(0), call(0.1)])


if __name__ == "__main__":
    unittest.main()

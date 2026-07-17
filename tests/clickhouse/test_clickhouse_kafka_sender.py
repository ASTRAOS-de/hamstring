import unittest
from unittest.mock import patch

from src.base.clickhouse_kafka_sender import ClickHouseKafkaSender


class TestInit(unittest.TestCase):
    @patch("src.base.clickhouse_kafka_sender.marshmallow_dataclass")
    @patch("src.base.clickhouse_kafka_sender.BufferedKafkaProduceHandler")
    def test_init(self, mock_produce_handler, mock_marshmallow):
        # Arrange
        table_name = "test_table"
        mock_produce_handler_instance = mock_produce_handler
        mock_produce_handler.return_value = mock_produce_handler_instance

        # Act
        sut = ClickHouseKafkaSender(table_name)

        # Assert
        self.assertEqual(table_name, sut.table_name)
        self.assertEqual(mock_produce_handler_instance, sut.kafka_producer)
        mock_produce_handler.assert_called_once()

    @patch("src.base.clickhouse_kafka_sender.marshmallow_dataclass")
    @patch("src.base.clickhouse_kafka_sender.BufferedKafkaProduceHandler")
    def test_init_uses_provided_producer(self, mock_produce_handler, mock_marshmallow):
        # Arrange
        table_name = "test_table"
        kafka_producer = object()

        # Act
        sut = ClickHouseKafkaSender(table_name, kafka_producer)

        # Assert
        self.assertEqual(table_name, sut.table_name)
        self.assertIs(kafka_producer, sut.kafka_producer)
        mock_produce_handler.assert_not_called()


class TestInsert(unittest.TestCase):
    @patch("src.base.clickhouse_kafka_sender.marshmallow_dataclass")
    @patch("src.base.clickhouse_kafka_sender.BufferedKafkaProduceHandler")
    def test_insert(self, mock_produce_handler, mock_marshmallow):
        # Arrange
        mock_produce_handler_instance = mock_produce_handler
        mock_produce_handler.return_value = mock_produce_handler_instance
        sut = ClickHouseKafkaSender("test_table")

        # Act
        sut.insert({"test_key": "test_value"})

        # Assert
        mock_produce_handler_instance.produce.assert_called_once()


if __name__ == "__main__":
    unittest.main()

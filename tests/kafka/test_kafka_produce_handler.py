import unittest
from unittest.mock import patch

from src.base.kafka import KafkaProduceHandler


class ConcreteProducer(KafkaProduceHandler):
    def publish(self, records):
        return None

    def complete(self, records, consumer, consumed_messages):
        return None


class TestKafkaProduceHandler(unittest.TestCase):
    @patch("src.base.kafka.producer.Producer")
    def test_constructs_and_explicitly_closes_producer(self, producer_type):
        handler = ConcreteProducer({"bootstrap.servers": "kafka:9092"})

        producer_type.assert_called_once_with({"bootstrap.servers": "kafka:9092"})
        handler.close(3.5)
        producer_type.return_value.flush.assert_called_once_with(3.5)

    def test_base_type_is_abstract(self):
        with self.assertRaises(TypeError):
            KafkaProduceHandler({})


if __name__ == "__main__":
    unittest.main()

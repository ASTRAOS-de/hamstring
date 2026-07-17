import os
import unittest
from unittest.mock import patch

from src.base.kafka import (
    ExactlyOnceKafkaConsumeHandler,
    ExactlyOnceKafkaProduceHandler,
    KafkaConsumeHandler,
    KafkaProduceHandler,
    SimpleKafkaConsumeHandler,
    SimpleKafkaProduceHandler,
    build_consumer_group_id,
    build_transactional_id,
    create_pipeline_consumer,
    create_pipeline_producer,
)


class TestKafkaFactoryAndIdentity(unittest.TestCase):
    def test_build_consumer_group_id_is_topic_specific(self):
        with patch.dict(os.environ, {"GROUP_ID": "pipeline"}):
            self.assertEqual(
                "pipeline.input-b__input_a",
                build_consumer_group_id(["input-b", "input/a"]),
            )

    def test_build_transactional_id_contains_explicit_worker_id(self):
        with patch.dict(os.environ, {"KAFKA_TRANSACTIONAL_ID_PREFIX": "replica-2"}):
            transactional_id = build_transactional_id(
                stage="detector",
                consume_topic="input",
                instance_name="dga",
                worker_id="p1-t3",
            )

        self.assertEqual("replica-2.detector.dga.input.p1-t3", transactional_id)

    @patch("src.base.kafka.factory.SimpleKafkaConsumeHandler")
    def test_simple_consumer_factory(self, consumer_type):
        result = create_pipeline_consumer("input", mode="simple")

        consumer_type.assert_called_once_with("input")
        self.assertIs(result, consumer_type.return_value)

    @patch("src.base.kafka.factory.ExactlyOnceKafkaConsumeHandler")
    def test_exactly_once_consumer_factory(self, consumer_type):
        result = create_pipeline_consumer("input", mode="exactly_once")

        consumer_type.assert_called_once_with("input")
        self.assertIs(result, consumer_type.return_value)

    @patch("src.base.kafka.factory.SimpleKafkaProduceHandler")
    def test_simple_producer_factory(self, producer_type):
        result = create_pipeline_producer(
            stage="stage", consume_topic="input", mode="simple"
        )

        producer_type.assert_called_once_with()
        self.assertIs(result, producer_type.return_value)

    @patch("src.base.kafka.factory.ExactlyOnceKafkaProduceHandler")
    @patch("src.base.kafka.factory.build_transactional_id", return_value="tid")
    def test_exactly_once_producer_factory(self, build_id, producer_type):
        result = create_pipeline_producer(
            stage="stage",
            consume_topic="input",
            instance_name="instance",
            worker_id="p0-t1",
            mode="exactly_once",
        )

        build_id.assert_called_once_with(
            stage="stage",
            consume_topic="input",
            instance_name="instance",
            worker_id="p0-t1",
        )
        producer_type.assert_called_once_with(transactional_id="tid")
        self.assertIs(result, producer_type.return_value)

    def test_unknown_pipeline_mode_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "KAFKA_PIPELINE_MODE"):
            create_pipeline_consumer("input", mode="legacy")

    def test_factory_result_types_share_interfaces(self):
        self.assertTrue(issubclass(SimpleKafkaConsumeHandler, KafkaConsumeHandler))
        self.assertTrue(issubclass(ExactlyOnceKafkaConsumeHandler, KafkaConsumeHandler))
        self.assertTrue(issubclass(SimpleKafkaProduceHandler, KafkaProduceHandler))
        self.assertTrue(issubclass(ExactlyOnceKafkaProduceHandler, KafkaProduceHandler))


if __name__ == "__main__":
    unittest.main()

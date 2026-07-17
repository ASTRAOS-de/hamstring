import json
import unittest
from unittest.mock import MagicMock, patch

from confluent_kafka import KafkaError

from src.base.data_classes.batch import Batch
from src.base.kafka import (
    ConsumedKafkaMessage,
    KafkaConsumeHandler,
    KafkaMessageFetchException,
    decode_batch_record,
    decode_json_record,
)


def kafka_message(
    value="value", key=b"key", topic="input", partition=2, offset=7, headers=None
):
    message = MagicMock()
    message.error.return_value = None
    message.key.return_value = key
    message.value.return_value = None if value is None else value.encode("utf-8")
    message.topic.return_value = topic
    message.partition.return_value = partition
    message.offset.return_value = offset
    message.headers.return_value = headers
    return message


class TestKafkaConsumeHandler(unittest.TestCase):
    @patch("src.base.kafka.consumer.KafkaTopicManager")
    @patch("src.base.kafka.consumer.AdminClient")
    @patch("src.base.kafka.consumer.Consumer")
    def setUp(self, consumer_type, admin_type, ensure_topics):
        self.consumer_type = consumer_type
        self.admin_type = admin_type
        self.topic_manager_type = ensure_topics
        self.handler = KafkaConsumeHandler(["input-a", "input-b"])
        self.consumer = consumer_type.return_value

    def test_connect_ensures_topics_then_subscribes(self):
        self.topic_manager_type.assert_called_once_with(
            self.admin_type.return_value, self.handler.settings
        )
        self.topic_manager_type.return_value.ensure.assert_called_once_with(
            ["input-a", "input-b"]
        )
        self.consumer.subscribe.assert_called_once_with(["input-a", "input-b"])
        self.assertFalse(self.handler.conf["enable.auto.commit"])

    def test_consume_batch_returns_record_metadata_and_headers(self):
        self.consumer.consume.side_effect = [
            [kafka_message(headers=[("correlation", b"id")])],
            [],
        ]

        records = self.handler.consume_batch(max_messages=10, timeout_ms=1)

        self.assertEqual(
            [
                ConsumedKafkaMessage(
                    key="key",
                    value="value",
                    topic="input",
                    partition=2,
                    offset=7,
                    headers=(("correlation", b"id"),),
                )
            ],
            records,
        )

    def test_consume_batch_ignores_partition_eof(self):
        eof = MagicMock()
        error = MagicMock()
        error.code.return_value = KafkaError._PARTITION_EOF
        eof.error.return_value = error
        self.consumer.consume.return_value = [eof]

        self.assertEqual([], self.handler.consume_batch(timeout_ms=0))

    def test_consume_batch_treats_unknown_topic_as_transient(self):
        unavailable = MagicMock()
        error = MagicMock()
        error.code.return_value = KafkaError.UNKNOWN_TOPIC_OR_PART
        error.retriable.return_value = False
        unavailable.error.return_value = error
        self.consumer.consume.return_value = [unavailable]

        self.assertEqual([], self.handler.consume_batch(timeout_ms=0))

    def test_consume_batch_rejects_permanent_broker_error(self):
        failed = MagicMock()
        error = MagicMock()
        error.code.return_value = KafkaError.MSG_SIZE_TOO_LARGE
        error.retriable.return_value = False
        failed.error.return_value = error
        self.consumer.consume.return_value = [failed]

        with self.assertRaises(KafkaMessageFetchException):
            self.handler.consume_batch(timeout_ms=0)

    def test_commit_uses_highest_next_offset_per_partition(self):
        records = [
            ConsumedKafkaMessage(None, "a", "input", 1, 3),
            ConsumedKafkaMessage(None, "b", "input", 1, 5),
            ConsumedKafkaMessage(None, "c", "input", 2, 2),
        ]

        self.handler.commit(records)

        offsets = self.consumer.commit.call_args.kwargs["offsets"]
        self.assertEqual(
            {("input", 1, 6), ("input", 2, 3)},
            {(offset.topic, offset.partition, offset.offset) for offset in offsets},
        )
        self.assertFalse(self.consumer.commit.call_args.kwargs["asynchronous"])

    def test_empty_commit_is_noop(self):
        self.handler.commit([])
        self.consumer.commit.assert_not_called()

    def test_group_metadata_is_encapsulated(self):
        self.assertIs(
            self.handler.group_metadata(),
            self.consumer.consumer_group_metadata.return_value,
        )

    def test_close_is_explicit(self):
        self.handler.close()
        self.consumer.close.assert_called_once_with()


class TestRecordDecoding(unittest.TestCase):
    def test_decode_json_record_requires_object(self):
        record = ConsumedKafkaMessage(None, '["not", "an", "object"]', "t", 0, 0)

        with self.assertRaisesRegex(ValueError, "Unknown data format"):
            decode_json_record(record)

    def test_decode_batch_record_decodes_embedded_json_loglines(self):
        payload = {
            "batch_tree_row_id": "row",
            "batch_id": "7cc92111-7f1e-45c6-b872-d89c8fdfd8cc",
            "begin_timestamp": "2026-07-16T08:00:00",
            "end_timestamp": "2026-07-16T08:01:00",
            "data": [json.dumps({"src_ip": "192.0.2.1"})],
        }
        record = ConsumedKafkaMessage(None, json.dumps(payload), "t", 0, 0)

        batch = decode_batch_record(record)

        self.assertIsInstance(batch, Batch)
        self.assertEqual([{"src_ip": "192.0.2.1"}], batch.data)

    def test_decode_batch_record_rejects_scalar_data(self):
        record = ConsumedKafkaMessage(None, json.dumps({"data": "wrong"}), "t", 0, 0)

        with self.assertRaisesRegex(ValueError, "must be a list"):
            decode_batch_record(record)


if __name__ == "__main__":
    unittest.main()

import json
import unittest
from unittest.mock import MagicMock, call, patch

from src.logcollector.batch_handler import BatchAccumulator


class TestBatchAccumulator(unittest.TestCase):
    def setUp(self):
        self.sender_patch = patch(
            "src.logcollector.batch_handler.ClickHouseKafkaSender"
        )
        self.sender_type = self.sender_patch.start()
        self.addCleanup(self.sender_patch.stop)
        self.monitoring_producer = object()
        self.accumulator = BatchAccumulator(
            "collector", monitoring_kafka_producer=self.monitoring_producer
        )

    def test_uses_one_supplied_monitoring_producer(self):
        self.sender_type.create_shared_producer.assert_not_called()
        self.sender_type.assert_any_call("logline_timestamps", self.monitoring_producer)

    def test_add_message_delegates_and_returns_key_count(self):
        self.accumulator.batch = MagicMock()
        self.accumulator.batch.get_message_count_for_batch_key.return_value = 3
        message = json.dumps(
            {
                "logline_id": "7cc92111-7f1e-45c6-b872-d89c8fdfd8cc",
                "ts": "2026-07-16T08:00:00",
            }
        )

        result = self.accumulator.add_message("subnet", message)

        self.assertEqual(3, result)
        self.accumulator.batch.add_message.assert_called_once_with(
            "subnet", "7cc92111-7f1e-45c6-b872-d89c8fdfd8cc", message
        )
        statuses = [
            insert.args[0]["status"]
            for insert in self.accumulator.logline_timestamps.insert.call_args_list
            if "status" in insert.args[0]
        ]
        self.assertEqual(["in_process", "batched"], statuses)

    def test_complete_all_returns_only_current_packets(self):
        self.accumulator.batch = MagicMock()
        self.accumulator.batch.get_stored_keys.return_value = {"current", "expired"}
        packet = {"data": ["message"]}
        self.accumulator.batch.complete_batch.side_effect = lambda key: (
            packet if key == "current" else None
        )

        result = self.accumulator.complete_all()

        self.assertEqual([("current", packet)], result)
        self.assertCountEqual(
            [call("current"), call("expired")],
            self.accumulator.batch.complete_batch.call_args_list,
        )

    def test_completion_errors_propagate(self):
        self.accumulator.batch = MagicMock()
        self.accumulator.batch.get_stored_keys.return_value = {"broken"}
        self.accumulator.batch.complete_batch.side_effect = ValueError("bad timestamp")

        with self.assertRaisesRegex(ValueError, "bad timestamp"):
            self.accumulator.complete_all()


if __name__ == "__main__":
    unittest.main()

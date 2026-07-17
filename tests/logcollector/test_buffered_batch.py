import json
import unittest
import uuid
from unittest.mock import patch

from src.logcollector.batch_handler import BufferedBatch


def logline(timestamp: str, value: str) -> str:
    return json.dumps({"ts": timestamp, "value": value})


class TestBufferedBatch(unittest.TestCase):
    def setUp(self):
        self.sender_patch = patch(
            "src.logcollector.batch_handler.ClickHouseKafkaSender"
        )
        self.sender_type = self.sender_patch.start()
        self.addCleanup(self.sender_patch.stop)
        self.batch = BufferedBatch("collector", monitoring_kafka_producer=object())

    def test_add_message_creates_and_extends_key_batch(self):
        first_id = uuid.uuid4()
        second_id = uuid.uuid4()

        self.batch.add_message("subnet", first_id, logline("2026-07-16T08:00:00", "a"))
        self.batch.add_message("subnet", second_id, logline("2026-07-16T08:01:00", "b"))

        self.assertEqual(2, self.batch.get_message_count_for_batch())
        self.assertEqual(2, self.batch.get_message_count_for_batch_key("subnet"))
        self.assertEqual({"subnet"}, self.batch.get_stored_keys())
        self.assertIn("subnet", self.batch.batch_id)

    @patch(
        "src.logcollector.batch_handler.uuid.uuid4",
        return_value="row-id",
    )
    def test_complete_sorts_current_batch_and_moves_it_to_overlap(self, generate_id):
        later = logline("2026-07-16T08:02:00", "later")
        earlier = logline("2026-07-16T08:00:00", "earlier")
        self.batch.add_message("subnet", uuid.uuid4(), later)
        batch_id = self.batch.batch_id["subnet"]
        self.batch.add_message("subnet", uuid.uuid4(), earlier)

        result = self.batch.complete_batch("subnet")

        self.assertEqual("row-id", result["batch_tree_row_id"])
        self.assertEqual(batch_id, result["batch_id"])
        self.assertEqual([earlier, later], result["data"])
        self.assertEqual([earlier, later], self.batch.buffer["subnet"])
        self.assertNotIn("subnet", self.batch.batch)
        self.assertNotIn("subnet", self.batch.batch_id)

    def test_next_completion_includes_previous_overlap(self):
        previous = logline("2026-07-16T08:00:00", "previous")
        current = logline("2026-07-16T08:05:00", "current")
        self.batch.add_message("subnet", uuid.uuid4(), previous)
        self.batch.complete_batch("subnet")
        self.batch.add_message("subnet", uuid.uuid4(), current)

        result = self.batch.complete_batch("subnet")

        self.assertEqual([previous, current], result["data"])
        self.assertEqual([current], self.batch.buffer["subnet"])

    def test_completion_without_current_batch_expires_overlap(self):
        self.batch.buffer["subnet"] = [logline("2026-07-16T08:00:00", "old")]

        self.assertIsNone(self.batch.complete_batch("subnet"))
        self.assertNotIn("subnet", self.batch.buffer)

    def test_completion_of_unknown_key_is_noop(self):
        self.assertIsNone(self.batch.complete_batch("missing"))

    def test_invalid_message_is_not_hidden_as_empty_batch(self):
        self.batch.batch["subnet"] = ["not json"]
        self.batch.batch_id["subnet"] = uuid.uuid4()

        with self.assertRaises(json.JSONDecodeError):
            self.batch.complete_batch("subnet")

    def test_count_helpers_return_zero_for_unknown_key(self):
        self.assertEqual(0, self.batch.get_message_count_for_batch_key("missing"))
        self.assertEqual(0, self.batch.get_message_count_for_buffer_key("missing"))


if __name__ == "__main__":
    unittest.main()

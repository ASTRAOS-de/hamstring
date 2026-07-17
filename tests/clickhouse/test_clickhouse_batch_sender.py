import unittest
import uuid
from datetime import datetime
from typing import Optional
from unittest.mock import Mock, patch

from clickhouse_connect.driver.exceptions import OperationalError, ProgrammingError

from src.monitoring.clickhouse_batch_sender import ClickHouseBatchSender, Table


class TestTable(unittest.TestCase):
    def test_verify_accepts_expected_fields_and_optional_values(self):
        table = Table("test", {"name": str, "count": Optional[int]})
        table.verify({"name": "value", "count": 2})
        table.verify({"name": "value", "count": None})

    def test_verify_rejects_missing_or_unknown_fields(self):
        table = Table("test", {"name": str, "count": int})
        with self.assertRaises(ValueError):
            table.verify({"name": "value"})
        with self.assertRaises(ValueError):
            table.verify({"name": "value", "other": 2})

    def test_verify_rejects_wrong_type(self):
        with self.assertRaises(TypeError):
            Table("test", {"count": int}).verify({"count": "two"})


class TestClickHouseBatchSender(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.client_patch = patch(
            "src.monitoring.clickhouse_batch_sender.create_clickhouse_client",
            return_value=self.client,
        )
        self.client_patch.start()
        self.addCleanup(self.client_patch.stop)
        self.sender = ClickHouseBatchSender()

    def test_initializes_empty_batches_without_background_timer(self):
        self.assertEqual({name: [] for name in self.sender.tables}, self.sender.batch)
        self.assertFalse(hasattr(self.sender, "timer"))

    def test_add_appends_values_and_flushes_at_size_limit(self):
        self.sender.tables = {"test": Table("test", {"value": int})}
        self.sender.batch = {"test": []}
        self.sender.max_batch_size = 2

        self.sender.add("test", {"value": 1})
        self.sender.add("test", {"value": 2})

        self.client.insert.assert_called_once_with(
            "test", [[1], [2]], column_names=["value"]
        )
        self.assertEqual([], self.sender.batch["test"])

    def test_batch_tree_normalizes_absent_parent(self):
        data = {
            "batch_row_id": "root",
            "batch_id": uuid.UUID("5236b147-5b0d-44a8-981f-bd7da8c54733"),
            "parent_batch_row_id": None,
            "instance_name": "collector",
            "stage": "log_collection.batch_handler",
            "status": "completed",
            "timestamp": datetime(2026, 6, 8, 14, 1, 24),
        }

        self.sender.add("batch_tree", data)

        self.assertEqual("", data["parent_batch_row_id"])
        self.assertEqual("", self.sender.batch["batch_tree"][0][2])

    @patch("src.base.retry.time.sleep")
    def test_insert_reconnects_without_dropping_rows(self, sleep):
        first_client = Mock()
        second_client = Mock()
        first_client.insert.side_effect = OperationalError("unavailable")
        self.sender._client = first_client
        self.sender.tables = {"test": Table("test", {"value": int})}
        self.sender.batch = {"test": [[1]]}

        with patch.object(self.sender, "_connect_client", return_value=second_client):
            self.sender.insert("test")

        second_client.insert.assert_called_once_with(
            "test", [[1]], column_names=["value"]
        )
        self.assertEqual([], self.sender.batch["test"])
        sleep.assert_called()

    def test_permanent_insert_error_is_not_retried(self):
        self.client.insert.side_effect = ProgrammingError("invalid insert")
        self.sender.tables = {"test": Table("test", {"value": int})}
        self.sender.batch = {"test": [[1]]}

        with self.assertRaisesRegex(ProgrammingError, "invalid insert"):
            self.sender.insert("test")

        self.client.insert.assert_called_once()
        self.assertEqual([[1]], self.sender.batch["test"])

    def test_insert_all_flushes_each_table(self):
        with patch.object(self.sender, "insert") as insert:
            self.sender.insert_all()
        self.assertEqual(
            set(self.sender.batch), {call.args[0] for call in insert.call_args_list}
        )

    def test_close_flushes_and_closes_client(self):
        with patch.object(self.sender, "insert_all") as insert_all:
            self.sender.close()
        insert_all.assert_called_once_with()
        self.client.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()

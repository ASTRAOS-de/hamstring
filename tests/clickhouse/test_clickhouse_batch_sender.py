import unittest
import uuid
from datetime import datetime
from typing import Optional
from unittest.mock import patch, Mock

from src.monitoring.clickhouse_batch_sender import ClickHouseBatchSender, Table


class TestTable(unittest.TestCase):
    def test_verify_with_wrong_number_of_fields(self):
        # Arrange
        sut = Table(name="test_table", columns={"col1": str, "col2": int})

        # Act and Assert
        with self.assertRaises(ValueError):
            sut.verify({"col1": "value"})

    def test_verify_with_unexpected_field(self):
        # Arrange
        sut = Table(name="test_table", columns={"col1": str, "col2": int})

        # Act and Assert
        with self.assertRaises(ValueError):
            sut.verify({"col1": "value", "unexpected": 32})

    def test_verify_successful(self):
        # Arrange
        sut = Table(name="test_table", columns={"col1": str, "col2": int})

        # Act and Assert
        sut.verify({"col1": "value", "col2": 32})

    def test_verify_with_wrong_type(self):
        # Arrange
        sut = Table(name="test_table", columns={"col1": str, "col2": int})

        # Act and Assert
        with self.assertRaises(TypeError):
            sut.verify({"col1": 32, "col2": 32})

    def test_verify_with_optional_value(self):
        # Arrange
        sut = Table(name="test_table", columns={"col1": str, "col2": Optional[int]})

        # Act and Assert
        sut.verify({"col1": "value", "col2": 32})
        sut.verify({"col1": "value", "col2": None})

        with self.assertRaises(ValueError):
            sut.verify({"col1": "value"})


class TestInit(unittest.TestCase):
    def test_successful(self):
        # Act
        with (
            patch("src.monitoring.clickhouse_batch_sender.BATCH_SIZE", 50),
            patch("src.monitoring.clickhouse_batch_sender.BATCH_TIMEOUT", 0.5),
            patch(
                "src.monitoring.clickhouse_batch_sender.CLICKHOUSE_HOSTNAME",
                "test_name",
            ),
            patch(
                "src.monitoring.clickhouse_batch_sender.clickhouse_connect"
            ) as mock_clickhouse_connect,
        ):
            sut = ClickHouseBatchSender()

        # Assert
        self.assertIsNotNone(sut.tables)
        self.assertEqual(50, sut.max_batch_size)
        self.assertEqual(0.5, sut.batch_timeout)
        self.assertIsNone(sut.timer)
        self.assertIsNotNone(sut.lock)
        self.assertEqual({key: [] for key in sut.tables}, sut.batch)

    @patch("src.base.retry.time.sleep", return_value=None)
    def test_retries_until_clickhouse_is_available(self, mock_sleep):
        client = Mock()
        with patch(
            "src.monitoring.clickhouse_batch_sender.clickhouse_connect"
        ) as mock_clickhouse_connect:
            mock_clickhouse_connect.get_client.side_effect = [
                RuntimeError("clickhouse unavailable"),
                client,
            ]

            sut = ClickHouseBatchSender()

        self.assertEqual(client, sut._client)
        self.assertEqual(2, mock_clickhouse_connect.get_client.call_count)
        mock_sleep.assert_called()


class TestDel(unittest.TestCase):
    def setUp(self):
        with patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect"):
            self.sut = ClickHouseBatchSender()

    def test_del(self):
        # Act
        with patch(
            "src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert_all"
        ) as mock_insert_all:
            del self.sut

        # Assert
        mock_insert_all.assert_called_once()


class TestAdd(unittest.TestCase):
    def setUp(self):
        with patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect"):
            self.sut = ClickHouseBatchSender()

    def test_single_list_with_starting_timer(self):
        # Arrange
        test_table_name = "test_table"
        test_data = {"value_1": 1, "value_2": 2}

        self.sut.tables = {test_table_name: Table(test_table_name, {})}
        self.sut.batch = {test_table_name: []}

        # Act
        with (
            patch("src.monitoring.clickhouse_batch_sender.Table.verify"),
            patch(
                "src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender._start_timer"
            ) as mock_start_timer,
        ):
            self.sut.add(test_table_name, test_data)

        # Assert
        self.sut.batch = {test_table_name: [1, 2]}
        mock_start_timer.assert_called_once()

    def test_timer_already_started(self):
        # Arrange
        test_table_name = "test_table"
        test_data = {"value_1": 1, "value_2": 2}

        self.sut.tables = {test_table_name: Table(test_table_name, {})}
        self.sut.batch = {test_table_name: []}
        self.sut.timer = Mock()

        # Act
        with (
            patch("src.monitoring.clickhouse_batch_sender.Table.verify"),
            patch(
                "src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender._start_timer"
            ) as mock_start_timer,
        ):
            self.sut.add(test_table_name, test_data)

        # Assert
        mock_start_timer.assert_not_called()

    def test_max_batch_size_reached(self):
        # Arrange
        test_table_name = "test_table"
        test_data = {"value_1": 1, "value_2": 2}

        self.sut.tables = {test_table_name: Table(test_table_name, {})}
        self.sut.batch = {test_table_name: []}
        self.sut.max_batch_size = 1

        # Act
        with (
            patch("src.monitoring.clickhouse_batch_sender.Table.verify"),
            patch(
                "src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert"
            ) as mock_insert,
            patch(
                "src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender._start_timer"
            ),
        ):
            self.sut.add(test_table_name, test_data)

        # Assert
        mock_insert.assert_called_once_with(test_table_name)

    def test_batch_tree_parent_none_is_stored_as_empty_string(self):
        # Arrange
        test_data = {
            "batch_row_id": "root",
            "batch_id": uuid.UUID("5236b147-5b0d-44a8-981f-bd7da8c54733"),
            "parent_batch_row_id": None,
            "instance_name": "collector",
            "stage": "log_collection.batch_handler",
            "status": "completed",
            "timestamp": datetime(2026, 6, 8, 14, 1, 24),
        }

        # Act
        with patch(
            "src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender._start_timer"
        ):
            self.sut.add("batch_tree", test_data)

        # Assert
        self.assertEqual("", test_data["parent_batch_row_id"])
        self.assertEqual(
            "",
            self.sut.batch["batch_tree"][0][2],
        )


class TestInsert(unittest.TestCase):
    def setUp(self):
        with patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect"):
            self.sut = ClickHouseBatchSender()

    def test_filled_batch(self):
        # Arrange
        test_table_name = "test_table"

        self.sut.tables = {
            test_table_name: Table(test_table_name, {"col_1": str, "col_2": str})
        }
        self.sut.batch = {test_table_name: ["one", "two", "three"]}
        self.sut._client = Mock()

        # Act
        self.sut.insert(test_table_name)

        # Assert
        self.sut._client.insert.assert_called_once_with(
            test_table_name,
            ["one", "two", "three"],
            column_names=["col_1", "col_2"],
        )
        self.assertEqual([], self.sut.batch[test_table_name])

    @patch("src.base.retry.time.sleep", return_value=None)
    def test_filled_batch_retries_without_dropping_rows(self, mock_sleep):
        # Arrange
        test_table_name = "test_table"
        first_client = Mock()
        second_client = Mock()
        first_client.insert.side_effect = RuntimeError("clickhouse unavailable")

        self.sut.tables = {
            test_table_name: Table(test_table_name, {"col_1": str, "col_2": str})
        }
        self.sut.batch = {test_table_name: ["one", "two", "three"]}
        self.sut._client = first_client

        with patch.object(self.sut, "_connect_client", return_value=second_client):
            self.sut.insert(test_table_name)

        first_client.insert.assert_called_once()
        second_client.insert.assert_called_once_with(
            test_table_name,
            ["one", "two", "three"],
            column_names=["col_1", "col_2"],
        )
        self.assertEqual([], self.sut.batch[test_table_name])
        mock_sleep.assert_called()

    def test_empty_batch(self):
        # Arrange
        test_table_name = "test_table"

        self.sut.tables = {
            test_table_name: Table(test_table_name, {"col_1": str, "col_2": str})
        }
        self.sut.batch = {test_table_name: []}
        self.sut._client = Mock()

        # Act
        self.sut.insert(test_table_name)

        # Assert
        self.sut._client.insert.assert_not_called()
        self.assertEqual([], self.sut.batch[test_table_name])


class TestInsertAll(unittest.TestCase):
    def setUp(self):
        with patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect"):
            self.sut = ClickHouseBatchSender()

    def test_successful(self):
        # Arrange
        test_table_name_1 = "test_table_1"
        test_table_name_2 = "test_table_2"

        self.sut.tables = {
            test_table_name_1: Table(test_table_name_1, {}),
            test_table_name_2: Table(test_table_name_2, {}),
        }
        self.sut.batch = {test_table_name_1: [1, 2, 3], test_table_name_2: [4, 5]}
        self.sut.timer = Mock()

        # Act
        with patch(
            "src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert"
        ) as mock_insert:
            self.sut.insert_all()

        # Assert

        mock_insert.assert_any_call(test_table_name_1)
        mock_insert.assert_any_call(test_table_name_2)


class TestStartTimer(unittest.TestCase):
    def setUp(self):
        with patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect"):
            self.sut = ClickHouseBatchSender()

    def test_without_existing_timer(self):
        # Arrange
        self.sut.timer = None

        # Act
        with (
            patch("src.monitoring.clickhouse_batch_sender.Timer") as mock_timer,
            patch("src.monitoring.clickhouse_batch_sender.BATCH_TIMEOUT", 5),
            patch(
                "src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert_all"
            ) as mock_insert_all,
        ):
            self.sut._start_timer()

        # Assert
        mock_timer.assert_called_once_with(5, mock_insert_all)
        # noinspection PyUnresolvedReferences
        self.sut.timer.start.assert_called_once()

    def test_with_existing_timer(self):
        # Arrange
        self.sut.timer = Mock()

        # Act
        with (
            patch("src.monitoring.clickhouse_batch_sender.Timer") as mock_timer,
            patch("src.monitoring.clickhouse_batch_sender.BATCH_TIMEOUT", 5),
            patch(
                "src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert_all"
            ) as mock_insert_all,
            patch.object(self.sut.timer, "cancel") as mock_cancel,
        ):
            self.sut._start_timer()

        # Assert
        mock_cancel.assert_called_once()
        mock_timer.assert_called_once_with(5, mock_insert_all)
        # noinspection PyUnresolvedReferences
        self.sut.timer.start.assert_called_once()

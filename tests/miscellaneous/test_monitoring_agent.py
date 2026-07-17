import datetime
import os
import unittest
import uuid
from unittest.mock import patch, Mock, call, mock_open

import marshmallow_dataclass

from src.base.data_classes.clickhouse_connectors import ServerLogs
from src.base.kafka import ConsumedKafkaMessage
from src.monitoring.monitoring_agent import (
    CREATE_TABLES_DIRECTORY,
    MONITORING_CONSUMER_BATCH_SIZE,
    MONITORING_CONSUMER_TIMEOUT_MS,
    build_monitoring_worker,
    main,
    start_monitoring_workers,
)
from src.monitoring.monitoring_agent import MonitoringAgent, prepare_all_tables


class TestPrepareAllTables(unittest.TestCase):
    @patch("os.listdir", return_value=["table2.sql", "not_sql.txt", "table1.sql"])
    @patch("builtins.open", new_callable=mock_open, read_data="CREATE TABLE test;")
    @patch("clickhouse_connect.get_client")
    def test_prepare_all_tables(self, mock_get_client, mock_open_file, mock_listdir):
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value.__enter__.return_value = mock_client

        # Act
        prepare_all_tables()

        # Assert
        mock_listdir.assert_called_once_with(CREATE_TABLES_DIRECTORY)
        self.assertEqual(mock_open_file.call_count, 2)
        self.assertEqual(
            mock_open_file.call_args_list,
            [
                call(os.path.join(CREATE_TABLES_DIRECTORY, "table1.sql"), "r"),
                call(os.path.join(CREATE_TABLES_DIRECTORY, "table2.sql"), "r"),
            ],
        )
        mock_client.command.assert_called_with("CREATE TABLE test")
        self.assertEqual(mock_client.command.call_count, 2)

    @patch("os.listdir", return_value=["rollups.sql"])
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data="CREATE TABLE first;\n\nCREATE VIEW second AS SELECT 1;\n",
    )
    @patch("clickhouse_connect.get_client")
    def test_prepare_all_tables_executes_each_statement(
        self, mock_get_client, mock_open_file, mock_listdir
    ):
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value.__enter__.return_value = mock_client

        # Act
        prepare_all_tables()

        # Assert
        self.assertEqual(
            mock_client.command.call_args_list,
            [
                call("CREATE TABLE first"),
                call("CREATE VIEW second AS SELECT 1"),
            ],
        )

    @patch("os.listdir", return_value=["table1.sql"])
    @patch("builtins.open", new_callable=mock_open, read_data="CREATE TABLE test;")
    @patch("clickhouse_connect.get_client")
    def test_prepare_all_tables_with_exception(
        self, mock_get_client, mock_open_file, mock_listdir
    ):
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value.__enter__.return_value = mock_client

        mock_client.command.side_effect = Exception("Simulated Error")

        # Act
        with self.assertRaises(Exception) as context:
            prepare_all_tables()

        # Assert
        self.assertEqual(str(context.exception), "Simulated Error")


class TestInit(unittest.TestCase):
    def test_successful(self):
        # Act
        with (
            patch(
                "src.monitoring.monitoring_agent.SimpleKafkaConsumeHandler"
            ) as mock_simple_kafka_consume_handler,
            patch(
                "src.monitoring.monitoring_agent.ClickHouseBatchSender"
            ) as mock_clickhouse_batch_sender,
        ):
            sut = MonitoringAgent()

        # Assert
        self.assertTrue(
            isinstance(sut.table_names, list)
            and all(isinstance(e, str) for e in sut.table_names)
        )
        self.assertTrue(all(e.startswith("clickhouse_") for e in sut.topics))
        self.assertIsNotNone(sut.kafka_consumer)
        self.assertIsNotNone(sut.batch_sender)
        self.assertEqual(set(sut.table_names), set(sut.data_schemas))
        mock_simple_kafka_consume_handler.assert_called_once_with(sut.topics)
        mock_clickhouse_batch_sender.assert_called_once_with(use_timer=False)


class TestRun(unittest.TestCase):
    def setUp(self):
        with (
            patch("src.monitoring.monitoring_agent.SimpleKafkaConsumeHandler"),
            patch("src.monitoring.monitoring_agent.ClickHouseBatchSender"),
        ):
            self.sut = MonitoringAgent()

    def test_successful(self):
        # Arrange
        data_schema = marshmallow_dataclass.class_schema(ServerLogs)()
        fixed_id = uuid.UUID("35871c8c-ff72-44ad-a9b7-4f02cf92d484")
        timestamp_in = datetime.datetime(2025, 4, 3, 12, 32, 7, 264410)
        value = data_schema.dumps(
            {
                "message_id": fixed_id,
                "timestamp_in": timestamp_in,
                "message_text": "test_text",
            }
        )
        source_record = ConsumedKafkaMessage(
            key="test_key",
            value=value,
            topic="clickhouse_server_logs",
            partition=2,
            offset=7,
        )
        source_records = [source_record]

        with (
            patch(
                "src.monitoring.monitoring_agent.marshmallow_dataclass.class_schema"
            ) as mock_class_schema,
        ):
            self.sut.kafka_consumer.consume_batch.side_effect = [
                source_records,
                KeyboardInterrupt(),
            ]

            # Act
            self.sut.run()

        # Assert
        self.sut.kafka_consumer.consume_batch.assert_called_with(
            MONITORING_CONSUMER_BATCH_SIZE,
            MONITORING_CONSUMER_TIMEOUT_MS,
        )
        self.sut.batch_sender.add.assert_called_once_with(
            "server_logs",
            {
                "message_id": fixed_id,
                "timestamp_in": timestamp_in,
                "message_text": "test_text",
            },
        )
        self.assertEqual(2, self.sut.batch_sender.insert_all.call_count)
        self.sut.kafka_consumer.commit.assert_called_once_with(source_records)
        mock_class_schema.assert_not_called()


class TestScaling(unittest.IsolatedAsyncioTestCase):
    @patch("src.monitoring.monitoring_agent.start_pipeline_worker_replicas")
    async def test_start_uses_configured_worker_replicas(self, mock_start_replicas):
        await start_monitoring_workers()

        mock_start_replicas.assert_awaited_once()
        call_kwargs = mock_start_replicas.call_args.kwargs
        self.assertEqual("monitoring.agent", call_kwargs["module_name"])
        self.assertEqual("run", call_kwargs["target_name"])
        self.assertIs(build_monitoring_worker, call_kwargs["worker_factory"])

    def test_worker_factory_assigns_worker_id(self):
        with (
            patch("src.monitoring.monitoring_agent.SimpleKafkaConsumeHandler"),
            patch("src.monitoring.monitoring_agent.ClickHouseBatchSender"),
        ):
            worker = build_monitoring_worker("p0-t3")

        self.assertEqual("p0-t3", worker.worker_id)


class TestMain(unittest.TestCase):
    @patch(
        "src.monitoring.monitoring_agent.start_monitoring_workers",
        new_callable=Mock,
    )
    @patch("asyncio.run")
    def test_main(self, mock_asyncio_run, mock_start_monitoring_workers):
        # Act
        main()

        # Assert
        mock_start_monitoring_workers.assert_called_once_with()
        mock_asyncio_run.assert_called_once_with(
            mock_start_monitoring_workers.return_value
        )


if __name__ == "__main__":
    unittest.main()

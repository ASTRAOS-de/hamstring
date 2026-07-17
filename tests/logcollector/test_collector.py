import asyncio
import datetime
import ipaddress
import json
import unittest
import uuid
from unittest.mock import MagicMock, patch, AsyncMock, Mock

from src.logcollector.collector import LogCollector, main
from src.base.utils import setup_config

_PRODUCER_PATCHER = patch("src.logcollector.collector.create_pipeline_producer")


def setUpModule():
    _PRODUCER_PATCHER.start()


def tearDownModule():
    _PRODUCER_PATCHER.stop()


class TestInit(unittest.TestCase):
    @patch("src.logcollector.collector.create_pipeline_consumer")
    @patch("src.logcollector.collector.BatchAccumulator")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_valid_init(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        mock_batch_handler_instance = MagicMock()
        mock_logline_handler_instance = MagicMock()
        mock_kafka_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance
        mock_logline_handler.return_value = mock_logline_handler_instance
        mock_kafka_handler.return_value = mock_kafka_handler_instance

        sut = LogCollector(
            collector_name="test-collector",
            consume_topic="test_topic",
            produce_topics=["produce-topic"],
            protocol="dns",
            validation_config={},
        )

        self.assertEqual(mock_batch_handler_instance, sut.batch_handler)
        self.assertEqual(mock_logline_handler_instance, sut.logline_handler)
        self.assertEqual(mock_kafka_handler_instance, sut.kafka_consume_handler)

        mock_batch_handler.assert_called_once()
        mock_logline_handler.assert_called_once()
        mock_kafka_handler.assert_called_once_with("test_topic")


class TestStart(unittest.IsolatedAsyncioTestCase):
    @patch("src.logcollector.collector.logger")
    @patch("src.logcollector.collector.create_pipeline_consumer")
    @patch("src.logcollector.collector.BatchAccumulator")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def setUp(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_consume_handler,
        mock_logger,
    ):
        self.sut = LogCollector(
            collector_name="my-collector",
            consume_topic="consume-topic",
            produce_topics=["produce-topic"],
            protocol="dns",
            validation_config={},
        )

    @patch("src.logcollector.collector.create_pipeline_executor")
    @patch("src.logcollector.collector.asyncio.get_event_loop")
    async def test_start_successful_execution(
        self, mock_get_event_loop, mock_create_pipeline_executor
    ):
        # Arrange
        self.sut.fetch = MagicMock()
        mock_executor = MagicMock()
        mock_create_pipeline_executor.return_value = mock_executor
        mock_loop = MagicMock()
        mock_loop.run_in_executor = AsyncMock(return_value=None)
        mock_get_event_loop.return_value = mock_loop

        await self.sut.start()

        mock_create_pipeline_executor.assert_called_once()
        mock_loop.run_in_executor.assert_awaited_once_with(
            mock_executor, self.sut.fetch
        )
        mock_executor.shutdown.assert_called_once_with(wait=False, cancel_futures=True)


class _StopFetching(RuntimeError):
    """Raised inside the test to break the infinite fetch loop."""


class TestFetch(unittest.TestCase):
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.BatchAccumulator")
    @patch("src.logcollector.collector.create_pipeline_consumer")
    @patch("src.logcollector.collector.LogCollector.send")
    @patch("src.logcollector.collector.logger")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_handle_kafka_inputs(
        self,
        mock_clickhouse,
        mock_logger,
        mock_send,
        mock_kafka_consume,
        mock_batch_sender,
        mock_logline_handler,
    ):
        mock_consume_handler = MagicMock()
        source_record = MagicMock(value="value1")
        source_record.header_text.return_value = "server-message-id"
        mock_consume_handler.consume_batch.side_effect = [
            [source_record],
            _StopFetching(),
        ]
        mock_kafka_consume.return_value = mock_consume_handler
        mock_batch_sender.return_value.add_message.return_value = 1
        mock_send.return_value = ("subnet-1", "batched-message")
        self.sut = LogCollector(
            collector_name="my-collector",
            consume_topic="consume-topic",
            produce_topics=["produce-topic"],
            protocol="dns",
            validation_config={},
        )
        self.sut.batch_configuration["batch_size"] = 1
        self.sut._flush_pending_batches = MagicMock()

        with self.assertRaises(_StopFetching):
            self.sut.fetch()

        mock_send.assert_called_once()
        self.sut.batch_handler.add_message.assert_called_once_with(
            "subnet-1", "batched-message"
        )
        self.sut._flush_pending_batches.assert_called_once()


class TestSend(unittest.TestCase):
    def setUp(self):
        with (
            patch("src.logcollector.collector.asyncio.Queue"),
            patch("src.logcollector.collector.BatchAccumulator"),
            patch("src.logcollector.collector.LoglineHandler"),
            patch("src.logcollector.collector.create_pipeline_consumer"),
            patch("src.logcollector.collector.ClickHouseKafkaSender"),
        ):
            self.sut = LogCollector(
                collector_name="my-collector",
                consume_topic="consume-topic",
                produce_topics=["produce-topic"],
                protocol="dns",
                validation_config={},
            )

    def test_valid_logline(self):
        timestamp = datetime.datetime(2026, 2, 14, 16, 38, 6, 184006)
        message = "test_message"

        # Arrange
        mock_logline_handler = Mock()
        self.sut.logline_handler = mock_logline_handler.return_value
        self.sut.logline_handler.validate_logline_and_get_fields_as_json.side_effect = [
            ValueError
        ]

        # Act
        self.sut.send(timestamp_in=timestamp, message=message)

        # Assert
        self.sut.batch_handler.add_message.assert_not_called()

    def test_invalid_logline_records_failed_terminal_event_for_logserver_message(self):
        timestamp = datetime.datetime(2026, 2, 14, 16, 38, 6, 184006)
        message = "test_message"
        server_message_id = uuid.UUID("bd72ccb4-0ef2-4100-aa22-e787122d6875")

        self.sut.failed_protocol_loglines = MagicMock()
        self.sut.server_log_terminal_events = MagicMock()
        self.sut.logline_handler.validate_logline_and_get_fields_as_json.side_effect = [
            ValueError
        ]

        self.sut.send(
            timestamp_in=timestamp,
            message=message,
            server_message_id=str(server_message_id),
        )

        self.sut.server_log_terminal_events.insert.assert_called_once()
        terminal_event = self.sut.server_log_terminal_events.insert.call_args.args[0]
        self.assertEqual(server_message_id, terminal_event["message_id"])
        self.assertEqual("log_collection.collector", terminal_event["stage"])
        self.assertEqual("failed", terminal_event["status"])

    def test_invalid_logline(self):
        timestamp = datetime.datetime(2026, 2, 14, 16, 38, 6, 184006)
        message = "test_message"

        # Arrange
        mock_logline_handler = Mock()
        self.sut.logline_handler = mock_logline_handler.return_value
        self.sut.server_log_to_logline = MagicMock()
        self.sut.logline_handler.validate_logline_and_get_fields_as_json.return_value = {
            "ts": str(timestamp),
            "status_code": "test_status",
            "src_ip": "192.168.3.141",
            "record_type": "test_record_type",
        }

        # Act
        with (
            patch(
                "src.logcollector.collector.uuid.uuid4",
                return_value=uuid.UUID("da3aec7f-b355-4a2c-a2f4-2066d49431a5"),
            ),
        ):
            subnet_id, batched_message = self.sut.send(
                timestamp_in=timestamp,
                message=message,
                server_message_id="bd72ccb4-0ef2-4100-aa22-e787122d6875",
            )

        # Assert
        self.assertEqual("192.168.3.0_24", subnet_id)
        self.assertEqual(
            "da3aec7f-b355-4a2c-a2f4-2066d49431a5",
            json.loads(batched_message)["logline_id"],
        )
        self.sut.batch_handler.add_message.assert_not_called()
        self.sut.server_log_to_logline.insert.assert_called_once()
        server_log_to_logline = self.sut.server_log_to_logline.insert.call_args.args[0]
        self.assertEqual(
            uuid.UUID("bd72ccb4-0ef2-4100-aa22-e787122d6875"),
            server_log_to_logline["message_id"],
        )
        self.assertEqual(
            uuid.UUID("da3aec7f-b355-4a2c-a2f4-2066d49431a5"),
            server_log_to_logline["logline_id"],
        )
        self.assertIn("timestamp", server_log_to_logline)


class TestKafkaBatchSerialization(unittest.TestCase):
    def setUp(self):
        self.sut = object.__new__(LogCollector)
        self.sut.max_kafka_record_bytes = 60
        self.schema = MagicMock()
        self.schema.dumps.side_effect = lambda data: json.dumps(data)

    def test_splits_one_logical_batch_into_broker_safe_records(self):
        packets = self.sut._serialize_batch_packets(
            {"batch_id": "batch-1", "data": ["a" * 15, "b" * 15, "c" * 15]},
            self.schema,
        )

        self.assertGreater(len(packets), 1)
        self.assertEqual(
            ["a" * 15, "b" * 15, "c" * 15],
            [item for packet in packets for item in json.loads(packet)["data"]],
        )
        self.assertTrue(all(len(packet.encode("utf-8")) <= 60 for packet in packets))

    def test_rejects_one_logline_larger_than_the_record_limit(self):
        with self.assertRaisesRegex(ValueError, "One serialized logline packet"):
            self.sut._serialize_batch_packets(
                {"batch_id": "batch-1", "data": ["a" * 100]}, self.schema
            )

    def test_rechecks_large_logline_after_packet_rollover(self):
        with self.assertRaisesRegex(ValueError, "One serialized logline packet"):
            self.sut._serialize_batch_packets(
                {"batch_id": "batch-1", "data": ["small", "a" * 100]},
                self.schema,
            )


class TestGetSubnetId(unittest.TestCase):
    @patch("src.logcollector.collector.create_pipeline_consumer")
    @patch("src.logcollector.collector.BatchAccumulator")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_get_subnet_id_ipv4(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        test_address = ipaddress.IPv4Address("192.168.1.1")
        expected_result = f"192.168.1.0_24"
        sut = LogCollector(
            collector_name="my-collector",
            consume_topic="consume-topic",
            produce_topics=["produce-topic"],
            protocol="dns",
            validation_config={},
        )
        sut.batch_configuration = {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {"ipv4_prefix_length": 24, "ipv6_prefix_length": 64},
        }
        # Act
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.create_pipeline_consumer")
    @patch("src.logcollector.collector.BatchAccumulator")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_get_subnet_id_ipv4_zero(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        test_address = ipaddress.IPv4Address("0.0.0.0")
        expected_result = f"0.0.0.0_24"
        sut = LogCollector(
            collector_name="my-collector",
            consume_topic="consume-topic",
            produce_topics=["produce-topic"],
            protocol="dns",
            validation_config={},
        )
        sut.batch_configuration = {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {"ipv4_prefix_length": 24, "ipv6_prefix_length": 64},
        }
        # Act
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.create_pipeline_consumer")
    @patch("src.logcollector.collector.BatchAccumulator")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_get_subnet_id_ipv4_max(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        test_address = ipaddress.IPv4Address("255.255.255.255")
        expected_result = f"255.255.254.0_23"
        sut = LogCollector(
            collector_name="my-collector",
            consume_topic="consume-topic",
            produce_topics=["produce-topic"],
            protocol="dns",
            validation_config={},
        )
        sut.batch_configuration = {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {"ipv4_prefix_length": 23, "ipv6_prefix_length": 64},
        }
        # Act
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.create_pipeline_consumer")
    @patch("src.logcollector.collector.BatchAccumulator")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_get_subnet_id_ipv6(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        test_address = ipaddress.IPv6Address("2001:db8:85a3:1234:5678:8a2e:0370:7334")
        expected_result = f"2001:db8:85a3:1234::_64"
        sut = LogCollector(
            collector_name="my-collector",
            consume_topic="consume-topic",
            produce_topics=["produce-topic"],
            protocol="dns",
            validation_config={},
        )
        # Act
        sut.batch_configuration = {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {"ipv4_prefix_length": 24, "ipv6_prefix_length": 64},
        }
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.create_pipeline_consumer")
    @patch("src.logcollector.collector.BatchAccumulator")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_get_subnet_id_ipv6_zero(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        # Arrange
        test_address = ipaddress.IPv6Address("::")
        expected_result = f"::_64"
        sut = LogCollector(
            collector_name="my-collector",
            consume_topic="consume-topic",
            produce_topics=["produce-topic"],
            protocol="dns",
            validation_config={},
        )
        sut.batch_configuration = {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {"ipv4_prefix_length": 24, "ipv6_prefix_length": 64},
        }
        # Act
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.create_pipeline_consumer")
    @patch("src.logcollector.collector.BatchAccumulator")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_get_subnet_id_ipv6_max(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        # Arrange
        test_address = ipaddress.IPv6Address("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")
        expected_result = f"ffff:ffff:ffff::_48"
        sut = LogCollector(
            collector_name="my-collector",
            consume_topic="consume-topic",
            produce_topics=["produce-topic"],
            protocol="dns",
            validation_config={},
        )
        sut.batch_configuration = {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {"ipv4_prefix_length": 24, "ipv6_prefix_length": 48},
        }
        # Act
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.create_pipeline_consumer")
    @patch("src.logcollector.collector.BatchAccumulator")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_get_subnet_id_unsupported_type(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        # Arrange
        test_address = "192.168.1.1"  # String instead of IPv4Address or IPv6Address
        sut = LogCollector(
            collector_name="my-collector",
            consume_topic="consume-topic",
            produce_topics=["produce-topic"],
            protocol="dns",
            validation_config={},
        )
        sut.batch_configuration = {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {"ipv4_prefix_length": 24, "ipv6_prefix_length": 48},
        }
        # Act & Assert
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            sut._get_subnet_id(test_address)

    @patch("src.logcollector.collector.create_pipeline_consumer")
    @patch("src.logcollector.collector.BatchAccumulator")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_get_subnet_id_none(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        # Arrange
        test_address = None
        sut = LogCollector(
            collector_name="my-collector",
            consume_topic="consume-topic",
            produce_topics=["produce-topic"],
            protocol="dns",
            validation_config={},
        )
        sut.batch_configuration = {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {"ipv4_prefix_length": 24, "ipv6_prefix_length": 48},
        }

        # Act & Assert
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            sut._get_subnet_id(test_address)


class TestMain(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.cs = [
            {
                "name": "test_collector",
                "protocol_base": "dns",
                "required_log_information": [
                    ["ts", "Timestamp", "%Y-%m-%dT%H:%M:%S"],
                    [
                        "domain_name",
                        "RegEx",
                r"^(?=.{1,253}$)((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,63}$",
                    ],
                    ["src_ip", "IpAddress"],
                ],
            }
        ]

    @patch("src.logcollector.collector.logger")
    @patch(
        "src.logcollector.collector.start_pipeline_worker_replicas",
        new_callable=AsyncMock,
    )
    @patch("asyncio.create_task")
    @patch("asyncio.run")
    async def test_main(
        self,
        mock_asyncio_run,
        mock_asyncio_create_task,
        mock_start_workers,
        mock_logger,
    ):
        # Arrange

        mock_asyncio_create_task.side_effect = lambda coro: coro

        with patch("src.logcollector.collector.COLLECTORS", self.cs):
            await main()

        mock_start_workers.assert_awaited_once()
        args, kwargs = mock_asyncio_create_task.call_args_list[0]
        expected_call = args[0]
        mock_asyncio_create_task.assert_called_once_with(expected_call)


if __name__ == "__main__":
    unittest.main()

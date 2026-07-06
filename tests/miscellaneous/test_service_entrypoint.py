import unittest
from unittest.mock import MagicMock, patch

from src.base import service_entrypoint


class TestParsing(unittest.TestCase):
    def test_parse_dependency_names(self):
        self.assertEqual(
            ["kafka", "clickhouse"],
            service_entrypoint.parse_dependency_names(" kafka, ClickHouse ,, "),
        )

    def test_parse_host_port_list(self):
        self.assertEqual(
            [("kafka1", 19092), ("clickhouse-server", 8123)],
            service_entrypoint.parse_host_port_list(
                "kafka1:19092, clickhouse-server:8123"
            ),
        )

    def test_parse_host_port_list_rejects_invalid_endpoint(self):
        with self.assertRaises(ValueError):
            service_entrypoint.parse_host_port_list("kafka1")


class TestEndpointDiscovery(unittest.TestCase):
    def test_get_kafka_endpoints_from_config(self):
        config = {
            "environment": {
                "kafka_brokers": [
                    {"hostname": "kafka1", "internal_port": 19092},
                    {"hostname": "kafka2", "internal_port": "19093"},
                ]
            }
        }

        self.assertEqual(
            [("kafka1", 19092), ("kafka2", 19093)],
            service_entrypoint.get_kafka_endpoints(config),
        )

    @patch.dict(
        "os.environ",
        {"HAMSTRING_KAFKA_WAIT_ENDPOINTS": "localhost:9092,localhost:9093"},
    )
    def test_get_kafka_endpoints_from_env(self):
        self.assertEqual(
            [("localhost", 9092), ("localhost", 9093)],
            service_entrypoint.get_kafka_endpoints({}),
        )

    def test_get_clickhouse_endpoint_from_config(self):
        config = {
            "environment": {
                "monitoring": {
                    "clickhouse_server": {
                        "hostname": "clickhouse-server",
                        "http_port": 8124,
                    }
                }
            }
        }

        self.assertEqual(
            ("clickhouse-server", 8124),
            service_entrypoint.get_clickhouse_endpoint(config),
        )

    @patch.dict(
        "os.environ",
        {"HAMSTRING_CLICKHOUSE_WAIT_ENDPOINT": "localhost:18123"},
    )
    def test_get_clickhouse_endpoint_from_env(self):
        self.assertEqual(
            ("localhost", 18123),
            service_entrypoint.get_clickhouse_endpoint({}),
        )


class TestReadinessChecks(unittest.TestCase):
    @patch("src.base.service_entrypoint.socket.create_connection")
    def test_can_connect_to_open_socket(self, mock_create_connection):
        connection = MagicMock()
        mock_create_connection.return_value.__enter__.return_value = connection

        self.assertTrue(service_entrypoint.can_connect("127.0.0.1", 19092, 1))
        mock_create_connection.assert_called_once_with(("127.0.0.1", 19092), timeout=1)

    @patch("src.base.service_entrypoint.http.client.HTTPConnection")
    def test_can_ping_clickhouse(self, mock_http_connection):
        response = MagicMock()
        response.status = 200
        connection = mock_http_connection.return_value
        connection.getresponse.return_value = response

        self.assertTrue(service_entrypoint.can_ping_clickhouse("127.0.0.1", 8123, 1))
        mock_http_connection.assert_called_once_with("127.0.0.1", 8123, timeout=1)
        connection.request.assert_called_once_with("GET", "/ping")
        response.read.assert_called_once()
        connection.close.assert_called_once()


class TestMain(unittest.TestCase):
    @patch.dict("os.environ", {"HAMSTRING_WAIT_FOR": ""}, clear=False)
    @patch("src.base.service_entrypoint.os.execv")
    @patch("src.base.service_entrypoint.sys.argv", ["service_entrypoint.py", "app.py"])
    def test_main_execs_target(self, mock_execv):
        service_entrypoint.main()

        mock_execv.assert_called_once_with(
            service_entrypoint.sys.executable,
            [service_entrypoint.sys.executable, "app.py"],
        )


if __name__ == "__main__":
    unittest.main()

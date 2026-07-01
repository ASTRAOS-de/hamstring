import datetime
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.alerter.alerter import AlerterBase


class TestAlerter(AlerterBase):
    def process_alert(self):
        pass


class TestAlerterLogRotation(unittest.TestCase):
    def _build_alerter(self, log_file_path: str, retention_days=7):
        alerting_config = {
            "log_to_file": True,
            "log_file_path": log_file_path,
            "log_rotation": {
                "enabled": True,
                "retention_days": retention_days,
            },
            "log_to_kafka": False,
        }
        patches = [
            patch("src.alerter.alerter.ALERTING_CONFIG", alerting_config),
            patch("src.alerter.alerter.ExactlyOnceKafkaConsumeHandler"),
            patch("src.alerter.alerter.ClickHouseKafkaSender"),
        ]
        for active_patch in patches:
            active_patch.start()
            self.addCleanup(active_patch.stop)

        return TestAlerter({"name": "test"}, "alerts")

    def test_active_log_file_path_uses_current_day(self):
        alerter = self._build_alerter("/tmp/alerts.txt")

        active_path = alerter._get_active_log_file_path(
            datetime.datetime(2026, 6, 25, 12, 30)
        )

        self.assertEqual("/tmp/alerts-2026-06-25.txt", active_path)

    def test_cleanup_rotated_logs_keeps_configured_number_of_days(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base_log_path = Path(temp_dir) / "alerts.txt"
            for filename in (
                "alerts-2026-06-22.txt",
                "alerts-2026-06-23.txt",
                "alerts-2026-06-24.txt",
                "alerts-2026-06-25.txt",
                "alerts-other.txt",
            ):
                (Path(temp_dir) / filename).write_text("{}\n")

            alerter = self._build_alerter(str(base_log_path), retention_days=3)
            alerter.name = "test"

            alerter._cleanup_rotated_logs(today=datetime.date(2026, 6, 25))

            self.assertFalse((Path(temp_dir) / "alerts-2026-06-22.txt").exists())
            self.assertTrue((Path(temp_dir) / "alerts-2026-06-23.txt").exists())
            self.assertTrue((Path(temp_dir) / "alerts-2026-06-24.txt").exists())
            self.assertTrue((Path(temp_dir) / "alerts-2026-06-25.txt").exists())
            self.assertTrue((Path(temp_dir) / "alerts-other.txt").exists())

    def test_log_to_file_writes_to_rotated_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            alerter = self._build_alerter(str(Path(temp_dir) / "alerts.txt"))
            alerter.alert_data = {"src_ip": "192.0.2.1"}
            alerter._cleanup_rotated_logs = MagicMock()

            with patch(
                "src.alerter.alerter.datetime.datetime",
                wraps=datetime.datetime,
            ) as mock_datetime:
                mock_datetime.now.return_value = datetime.datetime(2026, 6, 25, 12, 30)
                alerter._log_to_file_action()

            log_file = Path(temp_dir) / "alerts-2026-06-25.txt"
            self.assertEqual('{"src_ip": "192.0.2.1"}\n', log_file.read_text())

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CREATE_TABLES = ROOT / "docker" / "create_tables"
DASHBOARDS = ROOT / "docker" / "grafana-provisioning" / "dashboards"


class TestMonitoringHistorySchema(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rollups = (CREATE_TABLES / "zz_monitoring_rollups.sql").read_text()
        cls.history = (CREATE_TABLES / "zzz_monitoring_history.sql").read_text()

    def test_uses_three_non_overlapping_retention_tiers(self):
        self.assertIn("INTERVAL 7 DAY", self.rollups)
        self.assertIn("INTERVAL 30 DAY", self.history)
        self.assertIn("INTERVAL 90 DAY", self.history)
        self.assertIn("CREATE OR REPLACE VIEW alerts_history", self.history)
        self.assertIn("CREATE OR REPLACE VIEW fill_levels_history", self.history)

    def test_latency_snapshots_are_refreshable_and_replaceable(self):
        for resolution in ("1m", "15m", "1h"):
            self.assertIn(
                f"CREATE TABLE IF NOT EXISTS pipeline_latency_{resolution}",
                self.history,
            )
            self.assertIn(
                f"CREATE MATERIALIZED VIEW IF NOT EXISTS "
                f"pipeline_latency_{resolution}_refresh",
                self.history,
            )
        self.assertEqual(3, self.history.count("ENGINE = ReplacingMergeTree"))
        self.assertEqual(3, self.history.count(" APPEND TO pipeline_latency_"))

    def test_latency_history_keeps_distribution_summary(self):
        for column in (
            "sample_count",
            "min_latency_us",
            "avg_latency_us",
            "p50_latency_us",
            "p95_latency_us",
            "p99_latency_us",
            "max_latency_us",
        ):
            self.assertIn(column, self.history)


class TestHistoricalDashboardQueries(unittest.TestCase):
    def test_alert_and_fill_dashboards_use_history_views(self):
        alerts = (DASHBOARDS / "alerts.json").read_text()
        overview = (DASHBOARDS / "overview.json").read_text()
        log_volumes = (DASHBOARDS / "log_volumes.json").read_text()

        self.assertNotIn("FROM alerts_1m", alerts + overview)
        self.assertIn("FROM alerts_history", alerts + overview)
        self.assertNotIn("FROM fill_levels_1m", log_volumes)
        self.assertIn("FROM fill_levels_history", log_volumes)

    def test_latency_dashboard_uses_historical_latency_views(self):
        latencies = (DASHBOARDS / "latencies.json").read_text()
        overview = (DASHBOARDS / "overview.json").read_text()

        self.assertNotIn("FROM batch_tree bt1", latencies + overview)
        self.assertIn("FROM pipeline_latency_values", latencies + overview)
        self.assertIn("FROM pipeline_transport_latency_values", latencies)
        self.assertIn("FROM pipeline_roundtrip_latency_values", latencies)


if __name__ == "__main__":
    unittest.main()

import os
import unittest
from unittest.mock import patch

from src.base.kafka import config as kafka_config


class TestTransactionBatchSettings(unittest.TestCase):
    def _settings(
        self,
        topics,
        stage=None,
        *,
        global_config=None,
        stage_config=None,
        topic_config=None,
        environment=None,
    ):
        with patch.dict(os.environ, environment or {}, clear=True), patch.object(
            kafka_config,
            "KAFKA_TRANSACTION_BATCH_CONFIG",
            global_config or {"size": 100, "timeout_ms": 50},
        ), patch.object(
            kafka_config,
            "KAFKA_TRANSACTION_BATCH_STAGE_CONFIG",
            stage_config or {},
        ), patch.object(
            kafka_config,
            "KAFKA_TRANSACTION_BATCH_TOPIC_CONFIG",
            topic_config or {},
        ):
            return kafka_config.transaction_batch_settings(topics, stage)

    def test_global_defaults_are_used_without_an_override(self):
        self.assertEqual(
            (100, 50),
            self._settings("pipeline-topic", stage="data_analysis.detector"),
        )

    def test_short_stage_name_overrides_global_defaults(self):
        self.assertEqual(
            (10, 25),
            self._settings(
                "pipeline-topic",
                stage="data_analysis.detector",
                stage_config={"detector": {"size": 10, "timeout_ms": 25}},
            ),
        )

    def test_fully_qualified_stage_name_overrides_short_stage_name(self):
        self.assertEqual(
            (7, 25),
            self._settings(
                "pipeline-topic",
                stage="data_analysis.detector",
                stage_config={
                    "detector": {"size": 10, "timeout_ms": 25},
                    "data_analysis.detector": {"size": 7},
                },
            ),
        )

    def test_exact_topic_overrides_stage_one_field_at_a_time(self):
        self.assertEqual(
            (3, 25),
            self._settings(
                "pipeline-inspector_to_detector-domainator",
                stage="data_analysis.detector",
                stage_config={"detector": {"size": 10, "timeout_ms": 25}},
                topic_config={"pipeline-inspector_to_detector-domainator": {"size": 3}},
            ),
        )

    def test_multi_topic_consumer_uses_most_conservative_values(self):
        self.assertEqual(
            (5, 30),
            self._settings(
                ["topic-a", "topic-b"],
                topic_config={
                    "topic-a": {"size": 20, "timeout_ms": 30},
                    "topic-b": {"size": 5, "timeout_ms": 70},
                },
            ),
        )

    def test_environment_variables_are_final_overrides(self):
        self.assertEqual(
            (42, 125),
            self._settings(
                "pipeline-topic",
                stage="data_analysis.detector",
                stage_config={"detector": {"size": 10, "timeout_ms": 25}},
                environment={
                    "KAFKA_TRANSACTION_BATCH_SIZE": "42",
                    "KAFKA_TRANSACTION_BATCH_TIMEOUT_MS": "125",
                },
            ),
        )

    def test_supplied_config_uses_safe_detector_and_alerter_overrides(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                (10, 50),
                kafka_config.transaction_batch_settings(
                    "pipeline-inspector_to_detector-domainator",
                    "data_analysis.detector",
                ),
            )
            self.assertEqual(
                (10, 50),
                kafka_config.transaction_batch_settings(
                    "pipeline-detector_to_detector-domainator-attributor",
                    "data_analysis.detector",
                ),
            )
            self.assertEqual(
                (25, 50),
                kafka_config.transaction_batch_settings(
                    "pipeline-detector_to_alerter-generic",
                    "pipeline.alerter",
                ),
            )


if __name__ == "__main__":
    unittest.main()

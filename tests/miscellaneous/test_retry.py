import unittest
from unittest.mock import MagicMock, patch

from src.base.retry import (
    RetrySettings,
    load_retry_settings,
    retry_forever,
    retry_with_timeout,
)


class TestRetrySettings(unittest.TestCase):
    def test_settings_are_loaded_from_existing_application_config(self):
        config = {
            "pipeline": {
                "resilience": {
                    "retry": {
                        "initial_delay_seconds": 2,
                        "max_delay_seconds": 12,
                        "backoff_multiplier": 3,
                        "jitter_seconds": 0.5,
                        "log_every_attempts": 7,
                    }
                }
            }
        }

        settings = load_retry_settings(config)

        self.assertEqual(
            RetrySettings(
                initial_delay_seconds=2,
                max_delay_seconds=12,
                backoff_multiplier=3,
                jitter_seconds=0.5,
                log_every_attempts=7,
            ),
            settings,
        )

    @patch("src.base.retry.time.sleep")
    def test_retry_reuses_preloaded_settings(self, mock_sleep):
        operation = MagicMock(side_effect=[RuntimeError("temporary"), "done"])
        settings = RetrySettings(
            initial_delay_seconds=0.25,
            max_delay_seconds=1,
            backoff_multiplier=2,
            jitter_seconds=0,
            log_every_attempts=1,
        )

        result = retry_forever(
            operation,
            "test operation",
            settings,
            retryable=(RuntimeError,),
        )

        self.assertEqual("done", result)
        self.assertEqual(2, operation.call_count)
        mock_sleep.assert_called_once_with(0.25)

    @patch("src.base.retry.time.sleep")
    @patch("src.base.retry.time.monotonic", side_effect=[0.0, 0.5, 1.1])
    def test_bounded_retry_reraises_after_deadline(self, _mock_time, mock_sleep):
        operation = MagicMock(side_effect=RuntimeError("still unavailable"))
        settings = RetrySettings(0.25, 1, 2, 0, 1)

        with self.assertRaisesRegex(RuntimeError, "still unavailable"):
            retry_with_timeout(
                operation,
                "bounded operation",
                settings,
                timeout_seconds=1,
                retryable=(RuntimeError,),
            )

        self.assertEqual(2, operation.call_count)
        mock_sleep.assert_called_once_with(0.25)


if __name__ == "__main__":
    unittest.main()

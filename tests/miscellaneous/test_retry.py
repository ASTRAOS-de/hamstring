import unittest
from unittest.mock import MagicMock, patch

from src.base.retry import RetrySettings, load_retry_settings, retry_forever


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


if __name__ == "__main__":
    unittest.main()

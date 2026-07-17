import unittest
from unittest.mock import Mock, call, patch

from src.base.retry import RetrySettings, load_retry_settings, retry_forever


class TestRetrySettings(unittest.TestCase):
    def test_load_retry_settings_uses_supplied_stage_config(self):
        settings = load_retry_settings(
            {
                "pipeline": {
                    "resilience": {
                        "retry": {
                            "initial_delay_seconds": 2,
                            "max_delay_seconds": 8,
                            "backoff_multiplier": 3,
                            "jitter_seconds": 0,
                            "log_every_attempts": 4,
                        }
                    }
                }
            }
        )

        self.assertEqual(RetrySettings(2.0, 8.0, 3.0, 0.0, 4), settings)

    @patch.dict("os.environ", {"HAMSTRING_RETRY_INITIAL_DELAY_SECONDS": "0"})
    def test_zero_delay_is_bounded_to_avoid_busy_spin(self):
        self.assertEqual(0.01, load_retry_settings({}).initial_delay_seconds)


class TestRetryForever(unittest.TestCase):
    def setUp(self):
        self.settings = RetrySettings(0.1, 1.0, 2.0, 0.0, 1)

    @patch("src.base.retry.time.sleep")
    def test_retries_with_bounded_exponential_backoff(self, sleep):
        operation = Mock(side_effect=[OSError("one"), OSError("two"), "ok"])

        result = retry_forever(
            operation,
            "operation",
            self.settings,
            retryable=(OSError,),
        )

        self.assertEqual("ok", result)
        self.assertEqual([call(0.1), call(0.2)], sleep.call_args_list)

    @patch("src.base.retry.time.sleep")
    def test_non_retryable_exception_is_propagated(self, sleep):
        with self.assertRaisesRegex(ValueError, "permanent"):
            retry_forever(
                lambda: (_ for _ in ()).throw(ValueError("permanent")),
                "operation",
                self.settings,
                retryable=(OSError,),
            )
        sleep.assert_not_called()

    @patch("src.base.retry.time.sleep")
    def test_retry_predicate_can_reject_exception(self, sleep):
        with self.assertRaises(OSError):
            retry_forever(
                lambda: (_ for _ in ()).throw(OSError("permanent")),
                "operation",
                self.settings,
                retryable=(OSError,),
                retry_if=lambda exception: False,
            )
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()

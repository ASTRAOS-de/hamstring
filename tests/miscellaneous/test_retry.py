import unittest
from unittest.mock import patch

from src.base import retry


class TestResilienceConfig(unittest.TestCase):
    def setUp(self):
        retry._load_resilience_config.cache_clear()
        retry._default_retry_settings.cache_clear()

    def tearDown(self):
        retry._load_resilience_config.cache_clear()
        retry._default_retry_settings.cache_clear()

    @patch("src.base.retry.setup_config")
    def test_resilience_config_is_cached_and_returns_a_copy(self, mock_setup_config):
        mock_setup_config.return_value = {
            "pipeline": {"resilience": {"retry": {"initial_delay_seconds": 2}}}
        }

        first = retry.resilience_config()
        first["initial_delay_seconds"] = 99
        second = retry.resilience_config()

        mock_setup_config.assert_called_once()
        self.assertEqual(2, second["initial_delay_seconds"])

    @patch("src.base.retry.setup_config")
    def test_retry_forever_reuses_validated_default_settings(self, mock_setup_config):
        mock_setup_config.return_value = {
            "pipeline": {"resilience": {"retry": {"initial_delay_seconds": 2}}}
        }

        self.assertEqual("first", retry.retry_forever(lambda: "first", "first"))
        self.assertEqual("second", retry.retry_forever(lambda: "second", "second"))

        mock_setup_config.assert_called_once()


if __name__ == "__main__":
    unittest.main()

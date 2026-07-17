import unittest
from unittest.mock import patch, mock_open, MagicMock

from src.base.utils import *


class TestSetupConfig(unittest.TestCase):
    @patch("src.base.utils.CONFIG_FILEPATH", "fake/path/config.yaml")
    @patch("builtins.open", new_callable=mock_open, read_data="some_yaml_data: value")
    @patch("yaml.safe_load", return_value={"some_yaml_data": "value"})
    def test_load_config_success(self, mock_yaml_safe_load, mock_open_file):
        result = setup_config()

        mock_open_file.assert_called_once_with("fake/path/config.yaml", "r")
        mock_yaml_safe_load.assert_called_once()
        self.assertEqual(result, {"some_yaml_data": "value"})

    @patch("src.base.utils.logger")
    @patch("src.base.utils.CONFIG_FILEPATH", "fake/path/config.yaml")
    @patch("builtins.open", side_effect=FileNotFoundError)
    def test_load_config_file_not_found(self, mock_open_file, mock_logger):
        with self.assertRaises(FileNotFoundError):
            setup_config()


class TestGetBatchConfiguration(unittest.TestCase):
    @patch("src.base.utils.setup_config")
    def test_no_matching_collector(self, mock_setup_config):
        # Arrange
        mock_config = {
            "pipeline": {
                "log_collection": {
                    "default_batch_handler_config": {
                        "batch_size": 100,
                        "batch_timeout": 5.0,
                    },
                    "collectors": [
                        {"name": "collector1", "protocol_base": "dns"},
                        {"name": "collector2", "protocol_base": "http"},
                    ],
                }
            }
        }
        mock_setup_config.return_value = mock_config

        # Act
        result = get_batch_configuration("non_existent_collector")

        # Assert
        expected = {"batch_size": 100, "batch_timeout": 5.0}
        self.assertEqual(result, expected)

    @patch("src.base.utils.setup_config")
    def test_matching_collector_no_override(self, mock_setup_config):
        # Arrange
        mock_config = {
            "pipeline": {
                "log_collection": {
                    "default_batch_handler_config": {
                        "batch_size": 100,
                        "batch_timeout": 5.0,
                    },
                    "collectors": [{"name": "test_collector", "protocol_base": "dns"}],
                }
            }
        }
        mock_setup_config.return_value = mock_config

        # Act
        result = get_batch_configuration("test_collector")

        # Assert
        expected = {"batch_size": 100, "batch_timeout": 5.0}
        self.assertEqual(result, expected)

    @patch("src.base.utils.setup_config")
    def test_matching_collector_with_full_override(self, mock_setup_config):
        # Arrange
        mock_config = {
            "pipeline": {
                "log_collection": {
                    "default_batch_handler_config": {
                        "batch_size": 100,
                        "batch_timeout": 5.0,
                    },
                    "collectors": [
                        {
                            "name": "test_collector",
                            "protocol_base": "dns",
                            "batch_handler_config_override": {
                                "batch_size": 200,
                                "batch_timeout": 10.0,
                            },
                        }
                    ],
                }
            }
        }
        mock_setup_config.return_value = mock_config

        # Act
        result = get_batch_configuration("test_collector")

        # Assert
        expected = {"batch_size": 200, "batch_timeout": 10.0}
        self.assertEqual(result, expected)

    @patch("src.base.utils.setup_config")
    def test_matching_collector_with_partial_override(self, mock_setup_config):
        # Arrange
        mock_config = {
            "pipeline": {
                "log_collection": {
                    "default_batch_handler_config": {
                        "batch_size": 100,
                        "batch_timeout": 5.0,
                    },
                    "collectors": [
                        {
                            "name": "test_collector",
                            "protocol_base": "dns",
                            "batch_handler_config_override": {"batch_size": 200},
                        }
                    ],
                }
            }
        }
        mock_setup_config.return_value = mock_config

        # Act
        result = get_batch_configuration("test_collector")

        # Assert
        expected = {"batch_size": 200, "batch_timeout": 5.0}
        self.assertEqual(result, expected)

    @patch("src.base.utils.setup_config")
    def test_multiple_collectors_with_same_name_first_has_override(
        self, mock_setup_config
    ):
        # Arrange
        mock_config = {
            "pipeline": {
                "log_collection": {
                    "default_batch_handler_config": {
                        "batch_size": 100,
                        "batch_timeout": 5.0,
                    },
                    "collectors": [
                        {
                            "name": "test_collector",
                            "protocol_base": "dns",
                            "batch_handler_config_override": {"batch_size": 200},
                        },
                        {"name": "test_collector", "protocol_base": "http"},
                    ],
                }
            }
        }
        mock_setup_config.return_value = mock_config

        # Act
        result = get_batch_configuration("test_collector")

        # Assert - should return the first match, which has an override
        expected = {"batch_size": 200, "batch_timeout": 5.0}
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()

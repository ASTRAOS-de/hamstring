import unittest
from unittest.mock import MagicMock, patch

from src.base.acceleration import (
    apply_model_acceleration,
    resolve_acceleration_config,
)


class TestAccelerationConfig(unittest.TestCase):
    @patch("src.base.acceleration.is_cuda_available", return_value=True)
    def test_auto_uses_cuda_when_available(self, mock_cuda_available):
        config = {
            "acceleration": {
                "enabled": True,
                "default": {
                    "device": "auto",
                    "backend": "auto",
                },
            }
        }

        result = resolve_acceleration_config(config)

        self.assertTrue(result.enabled)
        self.assertEqual("cuda:0", result.selected_device)
        self.assertTrue(result.uses_gpu)

    @patch("src.base.acceleration.is_cuda_available", return_value=False)
    def test_auto_falls_back_to_cpu_when_cuda_is_unavailable(
        self, mock_cuda_available
    ):
        config = {
            "acceleration": {
                "enabled": True,
                "fallback_to_cpu": True,
                "default": {
                    "device": "auto",
                    "backend": "auto",
                },
            }
        }

        result = resolve_acceleration_config(config)

        self.assertEqual("cpu", result.selected_device)
        self.assertFalse(result.uses_gpu)

    @patch("src.base.acceleration.is_cuda_available", return_value=True)
    def test_instance_config_overrides_global_default(self, mock_cuda_available):
        config = {
            "acceleration": {
                "enabled": True,
                "default": {
                    "device": "auto",
                    "backend": "auto",
                },
            }
        }
        instance_config = {
            "acceleration": {
                "device": "cpu",
                "backend": "sklearn",
            }
        }

        result = resolve_acceleration_config(config, instance_config)

        self.assertEqual("cpu", result.selected_device)
        self.assertEqual("sklearn", result.backend)

    @patch("src.base.acceleration.is_cuda_available", return_value=False)
    def test_cuda_without_fallback_raises(self, mock_cuda_available):
        config = {
            "acceleration": {
                "enabled": True,
                "fallback_to_cpu": False,
                "default": {
                    "device": "cuda:0",
                    "backend": "auto",
                },
            }
        }

        with self.assertRaises(RuntimeError):
            resolve_acceleration_config(config)


class TestApplyModelAcceleration(unittest.TestCase):
    @patch("src.base.acceleration.is_cuda_available", return_value=True)
    def test_model_with_to_method_is_moved_to_cuda(self, mock_cuda_available):
        acceleration = resolve_acceleration_config(
            {
                "acceleration": {
                    "enabled": True,
                    "default": {"device": "cuda:0", "backend": "torch"},
                }
            }
        )
        model = MagicMock()
        model.to.return_value = "cuda-model"

        result = apply_model_acceleration(model, acceleration)

        model.to.assert_called_once_with("cuda:0")
        self.assertEqual("cuda-model", result)

    @patch("src.base.acceleration.is_cuda_available", return_value=True)
    def test_xgboost_backend_sets_device_param(self, mock_cuda_available):
        acceleration = resolve_acceleration_config(
            {
                "acceleration": {
                    "enabled": True,
                    "default": {"device": "cuda:0", "backend": "xgboost"},
                }
            }
        )

        class Model:
            def __init__(self):
                self.params = None

            def set_params(self, **params):
                self.params = params

        model = Model()

        result = apply_model_acceleration(model, acceleration)

        self.assertEqual({"device": "cuda:0"}, model.params)
        self.assertIs(model, result)


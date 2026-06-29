import importlib.util
import os
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AccelerationConfig:
    enabled: bool
    requested_device: str
    selected_device: str
    backend: str
    fallback_to_cpu: bool
    reason: str

    @property
    def uses_gpu(self) -> bool:
        return self.enabled and self.selected_device.startswith("cuda")


def resolve_acceleration_config(
    pipeline_config: dict,
    instance_config: dict | None = None,
    *,
    component_name: str = "",
    logger=None,
) -> AccelerationConfig:
    acceleration_config = pipeline_config.get("acceleration", {})
    default_config = acceleration_config.get("default", {})
    instance_config = instance_config or {}
    instance_acceleration_config = instance_config.get("acceleration", {})

    enabled = _coalesce(
        instance_acceleration_config.get("enabled"),
        acceleration_config.get("enabled"),
        False,
    )
    requested_device = str(
        _coalesce(
            instance_acceleration_config.get("device"),
            default_config.get("device"),
            "cpu",
        )
    )
    backend = str(
        _coalesce(
            instance_acceleration_config.get("backend"),
            default_config.get("backend"),
            "auto",
        )
    )
    fallback_to_cpu = bool(
        _coalesce(
            instance_acceleration_config.get("fallback_to_cpu"),
            acceleration_config.get("fallback_to_cpu"),
            default_config.get("fallback_to_cpu"),
            True,
        )
    )

    selected_device, reason = _select_device(
        enabled=enabled,
        requested_device=requested_device,
        fallback_to_cpu=fallback_to_cpu,
    )
    resolved_config = AccelerationConfig(
        enabled=bool(enabled),
        requested_device=requested_device,
        selected_device=selected_device,
        backend=backend,
        fallback_to_cpu=fallback_to_cpu,
        reason=reason,
    )

    if logger:
        logger.info(
            "%s acceleration resolved: enabled=%s backend=%s requested_device=%s selected_device=%s reason=%s",
            component_name or "pipeline",
            resolved_config.enabled,
            resolved_config.backend,
            resolved_config.requested_device,
            resolved_config.selected_device,
            resolved_config.reason,
        )

    return resolved_config


def apply_model_acceleration(
    model: Any,
    acceleration: AccelerationConfig,
    logger=None,
):
    if not acceleration.enabled:
        return model

    if not acceleration.uses_gpu:
        if logger:
            logger.info(
                "Using CPU for model %s: %s",
                type(model).__name__,
                acceleration.reason,
            )
        return model

    backend = _infer_backend(model, acceleration.backend)

    if hasattr(model, "to"):
        try:
            accelerated_model = model.to(acceleration.selected_device)
            if logger:
                logger.info(
                    "Moved model %s to %s via .to().",
                    type(model).__name__,
                    acceleration.selected_device,
                )
            return accelerated_model
        except Exception as e:
            if not acceleration.fallback_to_cpu:
                raise
            if logger:
                logger.warning(
                    "Could not move model %s to %s: %s. Keeping CPU model.",
                    type(model).__name__,
                    acceleration.selected_device,
                    e,
                )
            return model

    if backend == "xgboost" and hasattr(model, "set_params"):
        return _set_model_params(
            model,
            {"device": acceleration.selected_device},
            acceleration,
            logger,
            "XGBoost",
        )

    if backend == "lightgbm" and hasattr(model, "set_params"):
        return _set_model_params(
            model,
            {"device_type": "gpu"},
            acceleration,
            logger,
            "LightGBM",
        )

    if logger:
        logger.info(
            "Model %s does not expose a supported GPU offload API for backend '%s'. Keeping CPU model.",
            type(model).__name__,
            backend,
        )
    return model


def _select_device(
    *,
    enabled: bool,
    requested_device: str,
    fallback_to_cpu: bool,
) -> tuple[str, str]:
    requested_device = requested_device.strip().lower()
    if not enabled:
        return "cpu", "acceleration disabled"
    if requested_device == "cpu":
        return "cpu", "cpu requested"

    cuda_available = is_cuda_available()
    if requested_device == "auto":
        if cuda_available:
            return "cuda:0", "cuda available"
        return "cpu", "cuda unavailable; cpu fallback"

    if requested_device.startswith("cuda"):
        if cuda_available:
            return requested_device, "requested cuda device available"
        if fallback_to_cpu:
            return "cpu", f"{requested_device} unavailable; cpu fallback"
        raise RuntimeError(
            f"Requested acceleration device {requested_device} is not available."
        )

    if fallback_to_cpu:
        return "cpu", f"unsupported requested device '{requested_device}'; cpu fallback"
    raise RuntimeError(f"Unsupported acceleration device '{requested_device}'.")


def is_cuda_available() -> bool:
    if importlib.util.find_spec("torch") is not None:
        try:
            import torch

            return bool(torch.cuda.is_available())
        except Exception:
            pass

    visible_devices = os.environ.get("NVIDIA_VISIBLE_DEVICES", "")
    cuda_devices = os.environ.get("CUDA_VISIBLE_DEVICES", "")
    for value in (visible_devices, cuda_devices):
        normalized_value = value.strip().lower()
        if normalized_value and normalized_value not in {"none", "void", "-1"}:
            return True
    return False


def _coalesce(*values):
    for value in values:
        if value is not None:
            return value
    return None


def _infer_backend(model: Any, configured_backend: str) -> str:
    configured_backend = configured_backend.lower()
    if configured_backend != "auto":
        return configured_backend

    model_module = type(model).__module__.lower()
    if "xgboost" in model_module:
        return "xgboost"
    if "lightgbm" in model_module:
        return "lightgbm"
    if "torch" in model_module:
        return "torch"
    if "onnxruntime" in model_module:
        return "onnxruntime"
    if "cuml" in model_module:
        return "cuml"
    if "sklearn" in model_module or "scikit" in model_module:
        return "sklearn"
    return "auto"


def _set_model_params(
    model,
    params: dict,
    acceleration: AccelerationConfig,
    logger,
    backend_name: str,
):
    try:
        model.set_params(**params)
        if logger:
            logger.info(
                "Configured %s model %s for GPU with params %s.",
                backend_name,
                type(model).__name__,
                params,
            )
    except Exception as e:
        if not acceleration.fallback_to_cpu:
            raise
        if logger:
            logger.warning(
                "Could not configure %s model %s for GPU: %s. Keeping existing model.",
                backend_name,
                type(model).__name__,
                e,
            )
    return model

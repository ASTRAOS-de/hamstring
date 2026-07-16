import os
import random
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Callable, TypeVar

from src.base.log_config import get_logger
from src.base.utils import setup_config

logger = get_logger("base.retry")

T = TypeVar("T")

_DEFAULT_CONFIG = {
    "initial_delay_seconds": 1.0,
    "max_delay_seconds": 30.0,
    "backoff_multiplier": 2.0,
    "jitter_seconds": 0.25,
    "log_every_attempts": 5,
}


@dataclass(frozen=True)
class RetrySettings:
    """Validated retry settings reused by the hot producer path."""

    initial_delay_seconds: float
    max_delay_seconds: float
    backoff_multiplier: float
    jitter_seconds: float
    log_every_attempts: int


@lru_cache(maxsize=1)
def _load_resilience_config() -> dict[str, Any]:
    """Load the retry settings once for the lifetime of this process.

    Retrying is used on successful high-volume producer calls as well as error
    paths. Reading and parsing ``config.yaml`` for every call can therefore
    become a throughput bottleneck. Configuration changes take effect after a
    normal service restart, like the rest of the process-level configuration.
    """
    config = setup_config()
    retry_config = (
        config.get("pipeline", {}).get("resilience", {}).get("retry", {})
        if isinstance(config, dict)
        else {}
    )
    merged = dict(_DEFAULT_CONFIG)
    if isinstance(retry_config, dict):
        merged.update(retry_config)
    return merged


def resilience_config() -> dict[str, Any]:
    """Return a copy of the cached retry settings.

    The copy prevents callers from mutating the cached process configuration.
    Tests that need fresh settings may call ``_load_resilience_config.cache_clear``.
    """
    return dict(_load_resilience_config())


def _settings_from_config(config: dict[str, Any]) -> RetrySettings:
    # A zero-delay retry loop can consume an entire core while Kafka or
    # ClickHouse is unavailable. Keep retries responsive without busy-spinning.
    initial_delay = max(0.01, _float_setting(config, "initial_delay_seconds"))
    return RetrySettings(
        initial_delay_seconds=initial_delay,
        max_delay_seconds=max(
            initial_delay, _float_setting(config, "max_delay_seconds")
        ),
        backoff_multiplier=max(
            1.0, _float_setting(config, "backoff_multiplier")
        ),
        jitter_seconds=max(0.0, _float_setting(config, "jitter_seconds")),
        log_every_attempts=max(1, _int_setting(config, "log_every_attempts")),
    )


@lru_cache(maxsize=1)
def _default_retry_settings() -> RetrySettings:
    """Return process-wide retry settings without per-message config work."""
    return _settings_from_config(_load_resilience_config())


def retry_forever(
    operation: Callable[[], T],
    description: str,
    retry_config: dict[str, Any] | None = None,
    retryable: tuple[type[BaseException], ...] = (Exception,),
) -> T:
    settings = (
        _settings_from_config(retry_config)
        if retry_config is not None
        else _default_retry_settings()
    )
    initial_delay = settings.initial_delay_seconds
    max_delay = settings.max_delay_seconds
    multiplier = settings.backoff_multiplier
    jitter = settings.jitter_seconds
    log_every = settings.log_every_attempts

    delay = initial_delay
    attempt = 0

    while True:
        try:
            return operation()
        except retryable as exception:
            attempt += 1
            if attempt == 1 or attempt % log_every == 0:
                logger.warning(
                    "%s failed on attempt %d: %s. Retrying in %.1fs.",
                    description,
                    attempt,
                    exception,
                    delay,
                )
            sleep_for = delay + (random.uniform(0, jitter) if jitter else 0)
            time.sleep(sleep_for)
            delay = min(max_delay, delay * multiplier)


def _float_setting(config: dict[str, Any], key: str) -> float:
    env_key = f"HAMSTRING_RETRY_{key.upper()}"
    value = os.getenv(env_key, config.get(key, _DEFAULT_CONFIG[key]))
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(_DEFAULT_CONFIG[key])


def _int_setting(config: dict[str, Any], key: str) -> int:
    env_key = f"HAMSTRING_RETRY_{key.upper()}"
    value = os.getenv(env_key, config.get(key, _DEFAULT_CONFIG[key]))
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(_DEFAULT_CONFIG[key])

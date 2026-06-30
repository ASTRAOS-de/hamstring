import os
import random
import time
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


def resilience_config() -> dict[str, Any]:
    config = setup_config()
    retry_config = config.get("pipeline", {}).get("resilience", {}).get("retry", {})
    merged = dict(_DEFAULT_CONFIG)
    merged.update(retry_config)
    return merged


def retry_forever(
    operation: Callable[[], T],
    description: str,
    retry_config: dict[str, Any] | None = None,
    retryable: tuple[type[BaseException], ...] = (Exception,),
) -> T:
    config = retry_config or resilience_config()
    initial_delay = _float_setting(config, "initial_delay_seconds")
    max_delay = _float_setting(config, "max_delay_seconds")
    multiplier = max(1.0, _float_setting(config, "backoff_multiplier"))
    jitter = max(0.0, _float_setting(config, "jitter_seconds"))
    log_every = max(1, _int_setting(config, "log_every_attempts"))

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

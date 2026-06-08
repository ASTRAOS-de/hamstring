from __future__ import annotations

from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PipelineExecutorConfig:
    executor: str = "thread"
    max_workers: int = 1


def get_pipeline_executor_config(
    config: dict[str, Any],
    module_name: str,
    instance_name: str | None = None,
) -> PipelineExecutorConfig:
    scaling = config.get("pipeline", {}).get("scaling", {})
    defaults = scaling.get("defaults", {})
    modules = scaling.get("modules", {})
    module_config = modules.get(module_name, scaling.get(module_name, {}))

    merged = {}
    merged.update(defaults)
    merged.update(_without_instances(module_config))

    if instance_name:
        instance_config = module_config.get("instances", {}).get(instance_name, {})
        merged.update(instance_config)

    return _parse_executor_config(merged)


def create_pipeline_executor(
    config: dict[str, Any],
    module_name: str,
    instance_name: str | None = None,
) -> Executor:
    executor_config = get_pipeline_executor_config(
        config=config,
        module_name=module_name,
        instance_name=instance_name,
    )
    if executor_config.executor == "process":
        return ProcessPoolExecutor(max_workers=executor_config.max_workers)

    prefix = _thread_name_prefix(module_name, instance_name)
    return ThreadPoolExecutor(
        max_workers=executor_config.max_workers,
        thread_name_prefix=prefix,
    )


def _without_instances(config: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in config.items() if key != "instances"}


def _parse_executor_config(config: dict[str, Any]) -> PipelineExecutorConfig:
    executor = _normalize_executor_name(
        config.get("executor", config.get("executor_type", config.get("type")))
    )
    if executor is None:
        executor = _infer_executor(config)

    max_workers = _read_max_workers(config, executor)
    return PipelineExecutorConfig(executor=executor, max_workers=max_workers)


def _normalize_executor_name(value: Any) -> str | None:
    if value is None:
        return None

    normalized = str(value).strip().lower().replace("_", "-")
    if normalized in {"thread", "threads", "thread-pool", "threadpool"}:
        return "thread"
    if normalized in {"process", "processes", "process-pool", "processpool"}:
        return "process"

    raise ValueError(
        "Pipeline executor must be one of: thread, threads, thread-pool, "
        "process, processes, process-pool"
    )


def _infer_executor(config: dict[str, Any]) -> str:
    if "processes" in config:
        return "process"
    return "thread"


def _read_max_workers(config: dict[str, Any], executor: str) -> int:
    executor_specific_key = "processes" if executor == "process" else "threads"
    worker_value = config.get(executor_specific_key)
    if worker_value is None:
        worker_value = config.get("max_workers", config.get("workers"))
    if worker_value is None:
        worker_value = 1

    try:
        max_workers = int(worker_value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Pipeline executor worker count must be an integer") from exc

    if max_workers < 1:
        raise ValueError("Pipeline executor worker count must be at least 1")
    return max_workers


def _thread_name_prefix(module_name: str, instance_name: str | None) -> str:
    suffix = f"{module_name}-{instance_name}" if instance_name else module_name
    return "hamstring-" + "".join(
        character if character.isalnum() else "-" for character in suffix
    )

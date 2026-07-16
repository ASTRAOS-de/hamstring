from __future__ import annotations

import asyncio
import os
from concurrent.futures import (
    FIRST_EXCEPTION,
    Executor,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import dataclass
from typing import Any, Callable

from src.base.eos import build_worker_id


@dataclass(frozen=True)
class PipelineExecutorConfig:
    executor: str = "thread"
    processes: int = 1
    threads_per_process: int = 1

    @property
    def max_workers(self) -> int:
        return self.total_workers

    @property
    def total_workers(self) -> int:
        return self.processes * self.threads_per_process


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
    if executor_config.executor in {"process", "hybrid"}:
        return ProcessPoolExecutor(max_workers=executor_config.processes)

    prefix = _thread_name_prefix(module_name, instance_name)
    return ThreadPoolExecutor(
        max_workers=executor_config.threads_per_process,
        thread_name_prefix=prefix,
    )


async def start_pipeline_worker_replicas(
    config: dict[str, Any],
    module_name: str,
    instance_name: str | None,
    worker_factory: Callable[[str], Any],
    target_name: str,
    process_entrypoint: Callable[..., None] | None = None,
    process_args: tuple[Any, ...] = (),
) -> None:
    executor_config = get_pipeline_executor_config(
        config=config,
        module_name=module_name,
        instance_name=instance_name,
    )
    _set_topic_min_partitions(executor_config.total_workers)

    if executor_config.executor == "thread":
        await _start_thread_workers(
            worker_factory=worker_factory,
            target_name=target_name,
            module_name=module_name,
            instance_name=instance_name,
            process_index=0,
            threads_per_process=executor_config.threads_per_process,
        )
        return

    if process_entrypoint is None:
        raise ValueError(
            "process_entrypoint is required for process and hybrid scaling"
        )

    loop = asyncio.get_running_loop()
    executor = ProcessPoolExecutor(max_workers=executor_config.processes)
    try:
        futures = [
            loop.run_in_executor(
                executor,
                process_entrypoint,
                process_index,
                executor_config.threads_per_process,
                *process_args,
            )
            for process_index in range(executor_config.processes)
        ]
        await asyncio.gather(*futures)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def run_thread_worker_pool(
    worker_factory: Callable[[str], Any],
    target_name: str,
    module_name: str,
    instance_name: str | None,
    process_index: int,
    threads_per_process: int,
) -> None:
    executor = ThreadPoolExecutor(
        max_workers=threads_per_process,
        thread_name_prefix=_thread_name_prefix(module_name, instance_name),
    )
    futures = []
    try:
        for thread_index in range(threads_per_process):
            worker_id = build_worker_id(process_index, thread_index)
            worker = worker_factory(worker_id)
            futures.append(executor.submit(getattr(worker, target_name)))

        done, _ = wait(futures, return_when=FIRST_EXCEPTION)
        for future in done:
            future.result()
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


async def _start_thread_workers(
    worker_factory: Callable[[str], Any],
    target_name: str,
    module_name: str,
    instance_name: str | None,
    process_index: int,
    threads_per_process: int,
) -> None:
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(
        max_workers=threads_per_process,
        thread_name_prefix=_thread_name_prefix(module_name, instance_name),
    )
    try:
        futures = []
        for thread_index in range(threads_per_process):
            worker_id = build_worker_id(process_index, thread_index)
            worker = worker_factory(worker_id)
            futures.append(loop.run_in_executor(executor, getattr(worker, target_name)))

        await asyncio.gather(*futures)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def _set_topic_min_partitions(total_workers: int) -> None:
    try:
        service_instances = int(os.getenv("NUMBER_OF_INSTANCES", "1"))
    except ValueError:
        service_instances = 1

    requested_partitions = max(1, total_workers * max(1, service_instances))
    try:
        current_partitions = int(os.getenv("KAFKA_TOPIC_MIN_PARTITIONS", "1"))
    except ValueError:
        current_partitions = 1
    os.environ["KAFKA_TOPIC_MIN_PARTITIONS"] = str(
        max(current_partitions, requested_partitions)
    )


def _without_instances(config: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in config.items() if key != "instances"}


def _parse_executor_config(config: dict[str, Any]) -> PipelineExecutorConfig:
    executor = _normalize_executor_name(
        config.get("executor", config.get("executor_type", config.get("type")))
    )
    if executor is None:
        executor = _infer_executor(config)
    elif (
        executor == "process"
        and _has_explicit_process_count(config)
        and _read_threads_per_process(config, default=1) > 1
    ):
        executor = "hybrid"

    processes = _read_process_count(config, executor)
    threads_per_process = _read_threads_per_process(
        config,
        default=1 if executor == "process" else None,
    )
    return PipelineExecutorConfig(
        executor=executor,
        processes=processes,
        threads_per_process=threads_per_process,
    )


def _normalize_executor_name(value: Any) -> str | None:
    if value is None:
        return None

    normalized = str(value).strip().lower().replace("_", "-")
    if normalized in {"thread", "threads", "thread-pool", "threadpool"}:
        return "thread"
    if normalized in {"process", "processes", "process-pool", "processpool"}:
        return "process"
    if normalized in {"hybrid", "mixed", "process-thread", "process-thread-pool"}:
        return "hybrid"

    raise ValueError(
        "Pipeline executor must be one of: thread, threads, thread-pool, "
        "process, processes, process-pool, hybrid"
    )


def _infer_executor(config: dict[str, Any]) -> str:
    if _has_explicit_process_count(config) and _has_explicit_thread_count(config):
        return "hybrid"
    if _has_explicit_process_count(config):
        return "process"
    return "thread"


def _read_process_count(config: dict[str, Any], executor: str) -> int:
    if executor == "thread":
        return 1

    worker_value = config.get("processes")
    if worker_value is None and executor == "process":
        worker_value = config.get("max_workers", config.get("workers"))
    if worker_value is None:
        worker_value = 1

    return _read_positive_int(worker_value)


def _read_threads_per_process(
    config: dict[str, Any],
    default: int | None,
) -> int:
    worker_value = config.get("threads_per_process")
    if worker_value is None:
        worker_value = config.get("threads")
    if worker_value is None:
        if default is None:
            worker_value = config.get("max_workers", config.get("workers"))
        else:
            worker_value = default
    if worker_value is None:
        worker_value = 1

    return _read_positive_int(worker_value)


def _read_positive_int(value: Any) -> int:
    try:
        parsed_value = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Pipeline executor worker count must be an integer") from exc

    if parsed_value < 1:
        raise ValueError("Pipeline executor worker count must be at least 1")
    return parsed_value


def _has_explicit_process_count(config: dict[str, Any]) -> bool:
    return "processes" in config


def _has_explicit_thread_count(config: dict[str, Any]) -> bool:
    return "threads" in config or "threads_per_process" in config


def _thread_name_prefix(module_name: str, instance_name: str | None) -> str:
    suffix = f"{module_name}-{instance_name}" if instance_name else module_name
    return "hamstring-" + "".join(
        character if character.isalnum() else "-" for character in suffix
    )

from __future__ import annotations

import asyncio
from concurrent.futures import (
    FIRST_EXCEPTION,
    Executor,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class PipelineExecutorConfig:
    executor: str = "thread"
    processes: int = 1
    threads_per_process: int = 1

    @property
    def total_workers(self) -> int:
        return self.processes * self.threads_per_process


def get_pipeline_executor_config(
    config: dict[str, Any],
    module_name: str,
    instance_name: str | None = None,
) -> PipelineExecutorConfig:
    scaling = config.get("pipeline", {}).get("scaling", {})
    unsupported_scaling_keys = set(scaling) - {"defaults", "modules"}
    if unsupported_scaling_keys:
        formatted_keys = ", ".join(sorted(unsupported_scaling_keys))
        raise ValueError(f"Unsupported pipeline scaling section(s): {formatted_keys}")

    defaults = scaling.get("defaults", {})
    modules = scaling.get("modules", {})
    module_config = modules.get(module_name, {})

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


def build_worker_id(process_index: int, thread_index: int) -> str:
    """Return the explicit identity for one process/thread worker."""
    return f"p{process_index}-t{thread_index}"


def _without_instances(config: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in config.items() if key != "instances"}


def _parse_executor_config(config: dict[str, Any]) -> PipelineExecutorConfig:
    supported_keys = {"executor", "processes", "threads_per_process"}
    unsupported_keys = set(config) - supported_keys
    if unsupported_keys:
        formatted_keys = ", ".join(sorted(unsupported_keys))
        raise ValueError(f"Unsupported pipeline scaling option(s): {formatted_keys}")

    executor = str(config.get("executor", "thread")).strip().lower()
    if executor not in {"thread", "process", "hybrid"}:
        raise ValueError(
            "Pipeline executor must be one of: thread, process, hybrid"
        )

    processes = (
        _read_positive_int(config.get("processes", 1))
        if executor in {"process", "hybrid"}
        else 1
    )
    threads_per_process = (
        _read_positive_int(config.get("threads_per_process", 1))
        if executor in {"thread", "hybrid"}
        else 1
    )
    return PipelineExecutorConfig(
        executor=executor,
        processes=processes,
        threads_per_process=threads_per_process,
    )


def _read_positive_int(value: Any) -> int:
    try:
        parsed_value = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Pipeline executor worker count must be an integer") from exc

    if parsed_value < 1:
        raise ValueError("Pipeline executor worker count must be at least 1")
    return parsed_value


def _thread_name_prefix(module_name: str, instance_name: str | None) -> str:
    suffix = f"{module_name}-{instance_name}" if instance_name else module_name
    return "hamstring-" + "".join(
        character if character.isalnum() else "-" for character in suffix
    )

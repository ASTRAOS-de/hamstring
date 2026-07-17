import unittest
import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from unittest.mock import patch

from src.base.execution import (
    create_pipeline_executor,
    get_pipeline_executor_config,
    start_pipeline_worker_replicas,
)


class TestPipelineExecutorConfig(unittest.TestCase):
    def test_defaults_are_used_when_module_is_missing(self):
        config = {
            "pipeline": {
                "scaling": {
                    "defaults": {
                        "executor": "thread",
                        "threads_per_process": 3,
                    }
                }
            }
        }

        result = get_pipeline_executor_config(config, "data_analysis.detector")

        self.assertEqual("thread", result.executor)
        self.assertEqual(3, result.total_workers)
        self.assertEqual(1, result.processes)
        self.assertEqual(3, result.threads_per_process)

    def test_module_overrides_defaults(self):
        config = {
            "pipeline": {
                "scaling": {
                    "defaults": {
                        "executor": "thread",
                        "threads_per_process": 1,
                    },
                    "modules": {
                        "data_analysis.detector": {
                            "executor": "process",
                            "processes": 4,
                        }
                    },
                }
            }
        }

        result = get_pipeline_executor_config(config, "data_analysis.detector")

        self.assertEqual("process", result.executor)
        self.assertEqual(4, result.total_workers)
        self.assertEqual(4, result.processes)
        self.assertEqual(1, result.threads_per_process)

    def test_instance_overrides_module(self):
        config = {
            "pipeline": {
                "scaling": {
                    "defaults": {
                        "executor": "thread",
                        "threads_per_process": 1,
                    },
                    "modules": {
                        "log_collection.collector": {
                            "threads_per_process": 2,
                            "instances": {
                                "dga_collector": {
                                    "threads_per_process": 5,
                                }
                            },
                        }
                    },
                }
            }
        }

        result = get_pipeline_executor_config(
            config, "log_collection.collector", "dga_collector"
        )

        self.assertEqual("thread", result.executor)
        self.assertEqual(5, result.total_workers)
        self.assertEqual(1, result.processes)
        self.assertEqual(5, result.threads_per_process)

    def test_hybrid_executor_uses_processes_and_threads(self):
        config = {
            "pipeline": {
                "scaling": {
                    "modules": {
                        "data_analysis.detector": {
                            "executor": "hybrid",
                            "processes": 2,
                            "threads_per_process": 4,
                        }
                    }
                }
            }
        }

        result = get_pipeline_executor_config(config, "data_analysis.detector")

        self.assertEqual("hybrid", result.executor)
        self.assertEqual(2, result.processes)
        self.assertEqual(4, result.threads_per_process)
        self.assertEqual(8, result.total_workers)

    def test_legacy_scaling_options_are_rejected(self):
        config = {
            "pipeline": {
                "scaling": {
                    "defaults": {"executor": "thread", "max_workers": 4}
                }
            }
        }

        with self.assertRaisesRegex(ValueError, "max_workers"):
            get_pipeline_executor_config(config, "data_analysis.detector")

    def test_flat_module_scaling_configuration_is_rejected(self):
        config = {
            "pipeline": {
                "scaling": {
                    "data_analysis.detector": {
                        "executor": "thread",
                        "threads_per_process": 4,
                    }
                }
            }
        }

        with self.assertRaisesRegex(ValueError, "data_analysis.detector"):
            get_pipeline_executor_config(config, "data_analysis.detector")

    def test_process_executor_is_created(self):
        config = {
            "pipeline": {
                "scaling": {
                    "modules": {
                        "data_analysis.detector": {
                            "executor": "process",
                            "processes": 2,
                        }
                    }
                }
            }
        }

        executor = create_pipeline_executor(config, "data_analysis.detector")
        try:
            self.assertIsInstance(executor, ProcessPoolExecutor)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    def test_thread_executor_is_created_by_default(self):
        executor = create_pipeline_executor({}, "log_filtering.prefilter")
        try:
            self.assertIsInstance(executor, ThreadPoolExecutor)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    def test_invalid_worker_count_raises(self):
        config = {
            "pipeline": {
                "scaling": {
                    "defaults": {
                        "threads_per_process": 0,
                    }
                }
            }
        }

        with self.assertRaises(ValueError):
            get_pipeline_executor_config(config, "log_collection.collector")

    def test_thread_worker_replicas_create_one_worker_per_thread(self):
        config = {
            "pipeline": {
                "scaling": {
                    "modules": {
                        "log_filtering.prefilter": {
                            "executor": "thread",
                            "threads_per_process": 3,
                        }
                    }
                }
            }
        }
        worker_ids = []

        class Worker:
            def __init__(self, worker_id):
                self.worker_id = worker_id

            def run_once(self):
                worker_ids.append(self.worker_id)

        async def run_workers():
            await start_pipeline_worker_replicas(
                config=config,
                module_name="log_filtering.prefilter",
                instance_name=None,
                worker_factory=lambda worker_id: Worker(worker_id),
                target_name="run_once",
            )

        asyncio.run(run_workers())

        self.assertEqual(["p0-t0", "p0-t1", "p0-t2"], sorted(worker_ids))

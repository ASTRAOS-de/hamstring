import unittest
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from src.base.execution import (
    create_pipeline_executor,
    get_pipeline_executor_config,
)


class TestPipelineExecutorConfig(unittest.TestCase):
    def test_defaults_are_used_when_module_is_missing(self):
        config = {
            "pipeline": {
                "scaling": {
                    "defaults": {
                        "executor": "thread",
                        "max_workers": 3,
                    }
                }
            }
        }

        result = get_pipeline_executor_config(config, "data_analysis.detector")

        self.assertEqual("thread", result.executor)
        self.assertEqual(3, result.max_workers)

    def test_module_overrides_defaults(self):
        config = {
            "pipeline": {
                "scaling": {
                    "defaults": {"executor": "thread", "max_workers": 1},
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
        self.assertEqual(4, result.max_workers)

    def test_instance_overrides_module(self):
        config = {
            "pipeline": {
                "scaling": {
                    "defaults": {"executor": "thread", "max_workers": 1},
                    "modules": {
                        "log_collection.collector": {
                            "threads": 2,
                            "instances": {
                                "dga_collector": {
                                    "threads": 5,
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
        self.assertEqual(5, result.max_workers)

    def test_process_executor_is_created(self):
        config = {
            "pipeline": {
                "scaling": {
                    "modules": {
                        "data_analysis.detector": {
                            "executor": "process",
                            "max_workers": 2,
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
                        "threads": 0,
                    }
                }
            }
        }

        with self.assertRaises(ValueError):
            get_pipeline_executor_config(config, "log_collection.collector")

import importlib
import os
import sys
import uuid
from datetime import datetime
from enum import Enum, unique
import asyncio
from abc import ABC, abstractmethod
import marshmallow_dataclass
import numpy as np
from streamad.util import StreamGenerator, CustomDS

sys.path.append(os.getcwd())
from src.base.clickhouse_kafka_sender import ClickHouseKafkaSender
from src.base.data_classes.batch import Batch
from src.base.utils import (
    setup_config,
    get_zeek_sensor_topic_base_names,
    generate_collisions_resistant_uuid,
)
from src.base.acceleration import resolve_acceleration_config
from src.base.kafka_handler import (
    ExactlyOnceKafkaConsumeHandler,
    ExactlyOnceKafkaProduceHandler,
    KafkaMessageFetchException,
)
from src.base.log_config import get_logger
from src.base.execution import (
    create_pipeline_executor,
    run_thread_worker_pool,
    start_pipeline_worker_replicas,
)

module_name = "data_inspection.inspector"
logger = get_logger(module_name)

config = setup_config()
PRODUCE_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"][
    "inspector_to_detector"
]
CONSUME_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"][
    "prefilter_to_inspector"
]
SENSOR_PROTOCOLS = get_zeek_sensor_topic_base_names(config)
PREFILTERS = config["pipeline"]["log_filtering"]
INSPECTORS = config["pipeline"]["data_inspection"]
COLLECTORS = config["pipeline"]["log_collection"]["collectors"]
DETECTORS = config["pipeline"]["data_analysis"]
PLUGIN_PATH = "src.inspector.plugins"


class InspectorAbstractBase(ABC):  # pragma: no cover
    @abstractmethod
    def __init__(self, consume_topic, produce_topics, config) -> None:
        pass

    @abstractmethod
    def inspect_anomalies(self) -> None:
        pass

    @abstractmethod
    def _get_models(self, models) -> list:
        pass

    @abstractmethod
    def subnet_is_suspicious(self) -> bool:
        pass


class InspectorBase(InspectorAbstractBase):
    """Finds anomalies in a batch of requests and produces it to the ``Detector``."""

    def __init__(self, consume_topic, produce_topics, config) -> None:
        """
        Initializes the InspectorBase with necessary configurations and connections.

        Sets up Kafka handlers, database connections, and configuration parameters based on
        the provided configuration. For non-NoInspector implementations, initializes model
        related parameters including mode, model configurations, thresholds, and time parameters.

        Args:
            consume_topic (str): Kafka topic to consume messages from
            produce_topics (list): List of Kafka topics to produce messages to
            config (dict): Configuration dictionary containing inspector settings

        Note:
            The "NoInspector" implementation skips model configuration initialization
            as it doesn't perform actual anomaly detection.
        """

        if not config["inspector_class_name"] == "NoInspector":
            self.mode = config["mode"]
            self.model_configurations = (
                config["models"] if "models" in config.keys() else None
            )
            self.anomaly_threshold = config["anomaly_threshold"]
            self.score_threshold = config["score_threshold"]
            self.time_type = config["time_type"]
            self.time_range = config["time_range"]
        self.name = config["name"]
        self.acceleration = resolve_acceleration_config(
            globals()["config"].get("pipeline", {}),
            config,
            component_name=f"{module_name}.{self.name}",
            logger=logger,
        )
        self.consume_topic = consume_topic
        self.produce_topics = produce_topics
        self.batch_id = None
        self.X = None
        self.key = None
        self.begin_timestamp = None
        self.end_timestamp = None

        self.messages = []
        self.anomalies = []

        self.kafka_consume_handler = ExactlyOnceKafkaConsumeHandler(self.consume_topic)
        self.kafka_produce_handler = ExactlyOnceKafkaProduceHandler()
        self.monitoring_kafka_producer = ClickHouseKafkaSender.create_shared_producer()

        # databases
        self.batch_tree = ClickHouseKafkaSender(
            "batch_tree", self.monitoring_kafka_producer
        )
        self.batch_timestamps = ClickHouseKafkaSender(
            "batch_timestamps", self.monitoring_kafka_producer
        )
        self.suspicious_batch_timestamps = ClickHouseKafkaSender(
            "suspicious_batch_timestamps", self.monitoring_kafka_producer
        )
        self.suspicious_batches_to_batch = ClickHouseKafkaSender(
            "suspicious_batches_to_batch", self.monitoring_kafka_producer
        )
        self.logline_timestamps = ClickHouseKafkaSender(
            "logline_timestamps", self.monitoring_kafka_producer
        )
        self.fill_levels = ClickHouseKafkaSender(
            "fill_levels", self.monitoring_kafka_producer
        )

        self.fill_levels.insert(
            dict(
                timestamp=datetime.now(),
                stage=module_name,
                entry_type="total_loglines",
                entry_count=0,
            )
        )

    def get_and_fill_data(self, source_message=None) -> None:
        """Consumes data from Kafka and stores it for processing.

        Fetches batch data from the configured Kafka topic and stores it in internal data structures.
        If the Inspector is already busy processing data, the consumption is skipped with a warning.
        Logs batch information and updates database entries for monitoring purposes.
        """
        if self.messages:
            logger.warning(
                "Inspector is busy: Not consuming new messages. Wait for the Inspector to finish the "
                "current workload."
            )
            return

        key, data = self.kafka_consume_handler.consume_as_object(source_message)
        if data:
            self.parent_row_id = data.batch_tree_row_id
            self.batch_id = data.batch_id
            self.begin_timestamp = data.begin_timestamp
            self.end_timestamp = data.end_timestamp
            self.messages = data.data
            self.key = key
        self.batch_timestamps.insert(
            dict(
                batch_id=self.batch_id,
                stage=module_name,
                status="in_process",
                instance_name=self.name,
                timestamp=datetime.now(),
                is_active=True,
                message_count=len(self.messages),
            )
        )

        row_id = generate_collisions_resistant_uuid()

        self.batch_tree.insert(
            dict(
                batch_row_id=row_id,
                stage=module_name,
                instance_name=self.name,
                status="in_process",
                timestamp=datetime.now(),
                parent_batch_row_id=self.parent_row_id,
                batch_id=self.batch_id,
            )
        )

        self.fill_levels.insert(
            dict(
                timestamp=datetime.now(),
                stage=module_name,
                entry_type="total_loglines",
                entry_count=len(self.messages),
            )
        )
        if not self.messages:
            logger.info(
                "Received message:\n"
                f"    ⤷  Empty data field: No unfiltered data available. Belongs to subnet_id {key}."
            )
        else:
            logger.info(
                "Received message:\n"
                f"    ⤷  Contains data field of {len(self.messages)} message(s). Belongs to subnet_id {key}."
            )

    def clear_data(self) -> None:
        """Clears all data from internal data structures.

        Resets messages, anomalies, feature matrix, and timestamps to prepare
        the Inspector for processing the next batch of data.
        """
        self.messages = []
        self.anomalies = []
        self.X = []
        self.begin_timestamp = None
        self.end_timestamp = None
        logger.debug("Cleared messages and timestamps. Inspector is now available.")

    def send_data(self):
        """Forwards anomalous data to the Detector for further analysis.

        Evaluates anomaly scores against the configured thresholds. If the proportion of
        anomalous time steps exceeds the threshold, groups messages by client IP and
        forwards each group as a suspicious batch to the Detector via Kafka. Otherwise,
        logs the batch as filtered out and updates monitoring databases.
        """
        row_id = generate_collisions_resistant_uuid()
        if self.subnet_is_suspicious():
            buckets = {}
            for message in self.messages:
                if message["src_ip"] in buckets.keys():
                    buckets[message["src_ip"]].append(message)
                else:
                    buckets[message["src_ip"]] = []
                    buckets.get(message["src_ip"]).append(message)

            for key, value in buckets.items():

                suspicious_batch_id = uuid.uuid4()  # generate new suspicious_batch_id

                self.suspicious_batches_to_batch.insert(
                    dict(
                        timestamp=datetime.now(),
                        suspicious_batch_id=suspicious_batch_id,
                        batch_id=self.batch_id,
                    )
                )

                data_to_send = {
                    "batch_tree_row_id": row_id,
                    "batch_id": suspicious_batch_id,
                    "begin_timestamp": self.begin_timestamp,
                    "end_timestamp": self.end_timestamp,
                    "data": value,
                }

                batch_schema = marshmallow_dataclass.class_schema(Batch)()

                # important to finish before sending, otherwise detector can process before finished here!
                self.suspicious_batch_timestamps.insert(
                    dict(
                        suspicious_batch_id=suspicious_batch_id,
                        src_ip=key,
                        stage=module_name,
                        instance_name=self.name,
                        status="finished",
                        timestamp=datetime.now(),
                        is_active=True,
                        message_count=len(value),
                    )
                )

                self.batch_tree.insert(
                    dict(
                        batch_row_id=row_id,
                        stage=module_name,
                        instance_name=self.name,
                        status="finished",
                        timestamp=datetime.now(),
                        parent_batch_row_id=self.parent_row_id,
                        batch_id=suspicious_batch_id,
                    )
                )
                for topic in self.produce_topics:
                    self.kafka_produce_handler.produce(
                        topic=topic,
                        data=batch_schema.dumps(data_to_send),
                        key=key,
                    )

        else:  # subnet is not suspicious

            self.batch_timestamps.insert(
                dict(
                    batch_id=self.batch_id,
                    stage=module_name,
                    instance_name=self.name,
                    status="filtered_out",
                    timestamp=datetime.now(),
                    is_active=False,
                    message_count=len(self.messages),
                )
            )

            logline_ids = set()
            for message in self.messages:
                logline_ids.add(message["logline_id"])

            for logline_id in logline_ids:
                self.logline_timestamps.insert(
                    dict(
                        logline_id=logline_id,
                        stage=module_name,
                        status="filtered_out",
                        timestamp=datetime.now(),
                        is_active=False,
                    )
                )

            self.batch_tree.insert(
                dict(
                    batch_row_id=row_id,
                    stage=module_name,
                    instance_name=self.name,
                    status="finished",
                    timestamp=datetime.now(),
                    parent_batch_row_id=self.parent_row_id,
                    batch_id=self.batch_id,
                )
            )
        self.fill_levels.insert(
            dict(
                timestamp=datetime.now(),
                stage=module_name,
                entry_type="total_loglines",
                entry_count=0,
            )
        )

    def inspect(self):
        """
        Executes the anomaly detection process with validation and fallback handling.

        This method:
        1. Validates that model configurations exist
        2. Logs a warning if multiple models are configured (only first is used)
        3. Retrieves the models through _get_models()
        4. Calls inspect_anomalies() to perform the actual detection

        Raises:
            NotImplementedError: If no model configurations are provided
        """
        if self.model_configurations == None or len(self.model_configurations) == 0:
            logger.warning("No model is set!")
            raise NotImplementedError(f"No model is set!")
        if len(self.model_configurations) > 1:
            logger.warning(
                f"Model List longer than 1. Only the first one is taken: {self.model_configurations[0]['model']}!"
            )
        self.models = self._get_models(self.model_configurations)
        self.inspect_anomalies()

    # TODO: test this!
    def bootstrap_inspection_process(self):
        """
        Implements the main inspection process loop that continuously:
        1. Fetches new data from Kafka
        2. Inspects the data for anomalies
        3. Sends suspicious data to detectors
        """
        logger.info(f"Starting {self.name}")
        while True:
            try:
                source_messages = self.kafka_consume_handler.consume_batch()
                if not source_messages:
                    continue

                with self.kafka_produce_handler.transaction_batch(
                    self.kafka_consume_handler, source_messages
                ):
                    for source_message in source_messages:
                        try:
                            self.get_and_fill_data(source_message)
                            self.inspect()
                            self.send_data()
                        finally:
                            self.clear_data()
            except KafkaMessageFetchException as e:  # pragma: no cover
                logger.debug(e)
            except IOError as e:
                logger.error(e)
                raise e
            except ValueError as e:
                logger.debug(e)
            except KeyboardInterrupt:
                logger.info(f" {self.consume_topic}  Closing down Inspector...")
                break

    async def start(self):  # pragma: no cover
        """
        Starts the inspector in an asynchronous context.

        This method runs the synchronous bootstrap_inspection_process() in a separate
        thread using run_in_executor, allowing the inspector to operate concurrently
        with other async components in the pipeline.
        """
        loop = asyncio.get_running_loop()
        executor = create_pipeline_executor(config, module_name, self.name)
        try:
            await loop.run_in_executor(executor, self.bootstrap_inspection_process)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)


def build_inspector_worker(
    inspector_config,
    consume_topic,
    produce_topics,
    worker_id=None,
):
    class_name = inspector_config["inspector_class_name"]
    plugin_module_name = f"{PLUGIN_PATH}.{inspector_config['inspector_module_name']}"
    plugin_module = importlib.import_module(plugin_module_name)
    inspector_class = getattr(plugin_module, class_name)
    worker = inspector_class(
        consume_topic=consume_topic,
        produce_topics=produce_topics,
        config=inspector_config,
    )
    worker.worker_id = worker_id
    return worker


def run_inspector_worker_process(
    process_index,
    threads_per_process,
    inspector_config,
    consume_topic,
    produce_topics,
):
    def worker_factory(worker_id):
        return build_inspector_worker(
            inspector_config=inspector_config,
            consume_topic=consume_topic,
            produce_topics=produce_topics,
            worker_id=worker_id,
        )

    run_thread_worker_pool(
        worker_factory=worker_factory,
        target_name="bootstrap_inspection_process",
        module_name=module_name,
        instance_name=inspector_config["name"],
        process_index=process_index,
        threads_per_process=threads_per_process,
    )


async def main():
    """
    Entry point for the Inspector module.

    This function:
    1. Iterates through all configured inspectors
    2. Creates the appropriate inspector instance based on configuration
    3. Starts each inspector as an asynchronous task
    4. Gathers all tasks to run them concurrently

    The function dynamically loads inspector classes from the plugin system
    based on configuration values, allowing for flexible extension of the
    inspection capabilities.

    """
    tasks = []
    for inspector in INSPECTORS:
        logger.info(inspector["name"])
        consume_topic = f"{CONSUME_TOPIC_PREFIX}-{inspector['name']}"
        produce_topics = [
            f"{PRODUCE_TOPIC_PREFIX}-{detector['name']}"
            for detector in DETECTORS
            if detector.get("inspector_name") == inspector["name"]
            and str(detector.get("consume_from", "")).strip().lower() != "detector"
        ]
        class_name = inspector["inspector_class_name"]
        plugin_module_name = f"{PLUGIN_PATH}.{inspector['inspector_module_name']}"
        logger.info(f"using {class_name} and {plugin_module_name}")

        def worker_factory(
            worker_id,
            inspector=inspector,
            consume_topic=consume_topic,
            produce_topics=produce_topics,
        ):
            return build_inspector_worker(
                inspector_config=inspector,
                consume_topic=consume_topic,
                produce_topics=produce_topics,
                worker_id=worker_id,
            )

        tasks.append(
            asyncio.create_task(
                start_pipeline_worker_replicas(
                    config=config,
                    module_name=module_name,
                    instance_name=inspector["name"],
                    worker_factory=worker_factory,
                    target_name="bootstrap_inspection_process",
                    process_entrypoint=run_inspector_worker_process,
                    process_args=(inspector, consume_topic, produce_topics),
                )
            )
        )
    await asyncio.gather(*tasks)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())

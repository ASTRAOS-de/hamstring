import datetime
import hashlib
import json
import os
import pickle
import sys
import tempfile
import asyncio
import numpy as np
import requests
import marshmallow_dataclass
from numpy import median
from abc import ABC, abstractmethod
import importlib

sys.path.append(os.getcwd())
from src.base.clickhouse_kafka_sender import ClickHouseKafkaSender
from src.base.data_classes.batch import Batch
from src.base.utils import setup_config, generate_collisions_resistant_uuid
from src.base.kafka_handler import (
    ExactlyOnceKafkaConsumeHandler,
    ExactlyOnceKafkaProduceHandler,
    KafkaMessageFetchException,
)
from src.base.log_config import get_logger
from src.base.execution import create_pipeline_executor

module_name = "data_analysis.detector"
logger = get_logger(module_name)

BUF_SIZE = 65536  # let's read stuff in 64kb chunks!

config = setup_config()
INSPECTORS = config["pipeline"]["data_inspection"]
DETECTORS = config["pipeline"]["data_analysis"]

PIPELINE_TOPIC_PREFIXES = config["environment"]["kafka_topics_prefix"]["pipeline"]
INSPECTOR_TO_DETECTOR_TOPIC_PREFIX = PIPELINE_TOPIC_PREFIXES["inspector_to_detector"]
DETECTOR_TO_ALERTER_TOPIC_PREFIX = PIPELINE_TOPIC_PREFIXES["detector_to_alerter"]
DETECTOR_TO_DETECTOR_TOPIC_PREFIX = PIPELINE_TOPIC_PREFIXES.get(
    "detector_to_detector", "pipeline-detector_to_detector"
)

# Backwards-compatible aliases for tests and external imports.
CONSUME_TOPIC_PREFIX = INSPECTOR_TO_DETECTOR_TOPIC_PREFIX
PRODUCE_TOPIC_PREFIX = DETECTOR_TO_ALERTER_TOPIC_PREFIX

PLUGIN_PATH = "src.detector.plugins"


def _normalize_topic_suffixes(topic_suffixes) -> list[str]:
    if topic_suffixes is None:
        return []
    if isinstance(topic_suffixes, str):
        return [suffix.strip() for suffix in topic_suffixes.split(",") if suffix.strip()]
    if isinstance(topic_suffixes, (list, tuple, set)):
        return [str(suffix).strip() for suffix in topic_suffixes if str(suffix).strip()]
    return [str(topic_suffixes).strip()]


def build_alerter_topics(detector_config: dict) -> list[str]:
    if detector_config.get("send_to_alerter") is False:
        return []

    topic_suffixes = detector_config.get("produce_topics", "")
    topic_suffixes = _normalize_topic_suffixes(topic_suffixes)
    if not topic_suffixes:
        topic_suffixes = ["generic"]

    return [
        f"{DETECTOR_TO_ALERTER_TOPIC_PREFIX}-{topic_suffix}"
        for topic_suffix in topic_suffixes
    ]


def build_downstream_detector_topics(detector_config: dict) -> list[str]:
    detector_names = []
    for config_key in ("next_detectors", "produce_detector_topics"):
        detector_names.extend(_normalize_topic_suffixes(detector_config.get(config_key)))

    return [
        f"{DETECTOR_TO_DETECTOR_TOPIC_PREFIX}-{detector_name}"
        for detector_name in dict.fromkeys(detector_names)
    ]


def detector_consumes_from_detector(detector_config: dict) -> bool:
    consume_from = str(detector_config.get("consume_from", "")).strip().lower()
    if consume_from:
        return consume_from == "detector"

    detector_source_keys = (
        "upstream_detector_name",
        "source_detector_name",
        "input_detector_name",
    )
    if any(detector_config.get(source_key) for source_key in detector_source_keys):
        return True

    return not detector_config.get("inspector_name")


def build_detector_consume_topic(detector_config: dict) -> str:
    if detector_consumes_from_detector(detector_config):
        return f"{DETECTOR_TO_DETECTOR_TOPIC_PREFIX}-{detector_config['name']}"
    return f"{INSPECTOR_TO_DETECTOR_TOPIC_PREFIX}-{detector_config['name']}"


class WrongChecksum(Exception):  # pragma: no cover
    """Raises when model checksum validation fails."""

    pass


class DetectorAbstractBase(ABC):  # pragma: no cover
    """
    Abstract base class for all detector implementations.

    This class defines the interface that all concrete detector implementations must follow.
    It provides the essential methods that need to be implemented for a detector to function
    within the pipeline.

    Subclasses must implement all abstract methods to ensure proper integration with the
    detection system.
    """

    @abstractmethod
    def __init__(
        self,
        detector_config,
        consume_topic,
        produce_topics=None,
        downstream_detector_topics=None,
    ) -> None:
        pass

    @abstractmethod
    def get_model_download_url(self):
        pass

    @abstractmethod
    def get_scaler_download_url(self):
        pass

    @abstractmethod
    def predict(self, message) -> np.ndarray:
        pass


class DetectorBase(DetectorAbstractBase):
    """
    Base implementation for detectors in the pipeline.

    This class provides a concrete implementation of the detector interface with
    common functionality shared across all detector types. It handles model
    management, data processing, Kafka communication, and result reporting.

    The class is designed to be extended by specific detector implementations
    that provide model-specific prediction logic.
    """

    def __init__(
        self,
        detector_config,
        consume_topic,
        produce_topics=None,
        downstream_detector_topics=None,
    ) -> None:
        """
        Initialize the detector with configuration and Kafka topic settings.

        Sets up all necessary components including model loading, Kafka handlers,
        and database connections.

        Args:
            detector_config (dict): Configuration dictionary containing detector-specific
                parameters such as name, model, checksum, and threshold.
            consume_topic (str): Kafka topic from which the detector will consume messages.
        """

        self.name = detector_config["name"]
        self.model_name = detector_config["model"]
        self.model = self.model_name
        self.checksum = detector_config["checksum"]
        self.threshold = detector_config["threshold"]

        self.consume_topic = consume_topic
        if produce_topics is None:
            self.produce_topics = [f"{DETECTOR_TO_ALERTER_TOPIC_PREFIX}-generic"]
        elif isinstance(produce_topics, str):
            self.produce_topics = _normalize_topic_suffixes(produce_topics)
        else:
            self.produce_topics = produce_topics

        if downstream_detector_topics is None:
            self.downstream_detector_topics = []
        elif isinstance(downstream_detector_topics, str):
            self.downstream_detector_topics = _normalize_topic_suffixes(
                downstream_detector_topics
            )
        else:
            self.downstream_detector_topics = downstream_detector_topics
        self.suspicious_batch_id = None
        self.key = None
        self.messages = []
        self.warnings = []
        self.begin_timestamp = None
        self.end_timestamp = None
        self.model_path = os.path.join(
            tempfile.gettempdir(), f"{self.model_name}_{self.checksum}_model.pickle"
        )
        self.scaler_path = os.path.join(
            tempfile.gettempdir(), f"{self.model_name}_{self.checksum}_scaler.pickle"
        )

        self.kafka_consume_handler = ExactlyOnceKafkaConsumeHandler(self.consume_topic)
        self.kafka_produce_handler = None

        self.model, self.scaler = self._get_model()

        # databases
        self.batch_tree = ClickHouseKafkaSender("batch_tree")
        self.suspicious_batch_timestamps = ClickHouseKafkaSender(
            "suspicious_batch_timestamps"
        )
        self.alerts = ClickHouseKafkaSender("alerts")
        self.logline_timestamps = ClickHouseKafkaSender("logline_timestamps")
        self.fill_levels = ClickHouseKafkaSender("fill_levels")

        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
                stage=module_name,
                entry_type="total_loglines",
                entry_count=0,
            )
        )

    def get_and_fill_data(self) -> None:
        """
        Consume data from Kafka and store it for processing.

        This method retrieves messages from the Kafka topic, processes them, and
        prepares the data for detection. It handles batch management, timestamp
        tracking, and database updates for monitoring purposes.

        The method also manages the flow of data through the pipeline by updating
        relevant database tables with processing status and metrics.
        """
        if self.messages:
            logger.warning(
                "Detector is busy: Not consuming new messages. Wait for the Detector to finish the "
                "current workload."
            )
            return
        key, data = self.kafka_consume_handler.consume_as_object()
        if data.data:
            self.parent_row_id = data.batch_tree_row_id
            self.suspicious_batch_id = data.batch_id
            self.begin_timestamp = data.begin_timestamp
            self.end_timestamp = data.end_timestamp
            self.messages = data.data
            self.key = key
        self.suspicious_batch_timestamps.insert(
            dict(
                suspicious_batch_id=self.suspicious_batch_id,
                src_ip=key,
                stage=module_name,
                instance_name=self.name,
                status="in_process",
                timestamp=datetime.datetime.now(),
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
                timestamp=datetime.datetime.now(),
                parent_batch_row_id=self.parent_row_id,
                batch_id=self.suspicious_batch_id,
            )
        )

        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
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

    def _sha256sum(self, file_path: str) -> str:
        """
        Calculate the SHA256 checksum of a file.

        This utility method reads a file in chunks and computes its SHA256 hash,
        which is used for model integrity verification.

        Args:
            file_path (str): Path to the file for which the checksum should be calculated.

        Returns:
            str: Hexadecimal string representation of the SHA256 checksum.
        """
        h = hashlib.sha256()

        with open(file_path, "rb") as file:
            while True:
                # Reading is buffered, so we can read smaller chunks.
                chunk = file.read(h.block_size)
                if not chunk:
                    break
                h.update(chunk)

        return h.hexdigest()

    def _get_model(self):
        """
        Download and validate the detection model.

        This method handles the model management process:
        1. Checks if the model already exists locally
        2. Downloads the model if not present
        3. Verifies the model's integrity using SHA256 checksum
        4. Loads the model for use in detection

        The method ensures that only verified models are used for detection to
        maintain system reliability.

        Returns:
            object: The loaded model object ready for prediction.

        Raises:
            WrongChecksum: If the downloaded model's checksum doesn't match the expected value.
            requests.HTTPError: If there's an error downloading the model.
        """
        logger.info(f"Get model: {self.model_name} with checksum {self.checksum}")
        scaler_download_url = self.get_scaler_download_url()

        if not os.path.isfile(self.model_path):
            model_download_url = self.get_model_download_url()
            logger.info(
                f"downloading model {self.model_name} from {model_download_url} with checksum {self.checksum}"
            )
            response = requests.get(model_download_url)
            response.raise_for_status()
            with open(self.model_path, "wb") as f:
                f.write(response.content)

        if scaler_download_url and not os.path.isfile(self.scaler_path):
            scaler_response = requests.get(scaler_download_url)
            scaler_response.raise_for_status()
            with open(self.scaler_path, "wb") as f:
                f.write(scaler_response.content)

        if scaler_download_url:
            with open(self.scaler_path, "rb") as input_file:
                scaler = pickle.load(input_file)
        else:
            scaler = None
        # Check file sha256
        local_checksum = self._sha256sum(self.model_path)

        if local_checksum != self.checksum:
            logger.warning(
                f"Checksum {self.checksum} SHA256 is not equal with new checksum {local_checksum}!"
            )
            raise WrongChecksum(
                f"Checksum {self.checksum} SHA256 is not equal with new checksum {local_checksum}!"
            )

        with open(self.model_path, "rb") as input_file:
            clf = pickle.load(input_file)

        return clf, scaler

    def detect(self) -> None:
        """
        Process messages to detect malicious requests.

        This method applies the detection model to each message in the current batch,
        identifies potential threats based on the model's predictions, and collects
        warnings for further processing.

        The detection uses a threshold to determine if a prediction indicates
        malicious activity, and only warnings exceeding this threshold are retained.

        Note:
            This method relies on the implementation of ``predict``of the rspective subclass
        """
        logger.info("Start detecting malicious requests.")
        for message in self.messages:
            y_pred = self.predict(message)
            logger.info(f"Prediction: {y_pred}")
            # TODO: DO NOT USE if TRUE for prod!!!
            if (
                True
            ):  # np.argmax(y_pred, axis=1) == 1 and y_pred[0][1] > self.threshold:
                logger.info("Append malicious request to warning.")
                warning = {
                    "request": message,
                    "probability": float(y_pred[0][1]),
                    # TODO: what is the use of this? not even json serializabel ?
                    # "model": self.model,
                    "name": self.name,
                    "sha256": self.checksum,
                }
                self.warnings.append(warning)

    def clear_data(self):
        """Clears the data in the internal data structures."""
        self.messages = []
        self.begin_timestamp = None
        self.end_timestamp = None
        self.warnings = []

    def send_warning(self) -> None:
        """
        Dispatch detected warnings to the appropriate systems.

        This method handles the reporting of detected threats by:
        1. Calculating an overall threat score
        2. Storing detailed warning information
        3. Updating database records with detection results
        4. Marking processed loglines with appropriate status

        The method updates multiple database tables to maintain the pipeline's
        state tracking and provides detailed information about detected threats.
        """
        logger.info("Store alert.")
        row_id = generate_collisions_resistant_uuid()
        downstream_messages = []
        if len(self.warnings) > 0:
            overall_score = median(
                [warning["probability"] for warning in self.warnings]
            )
            downstream_messages = self._get_downstream_messages()
            alert = {
                "overall_score": overall_score,
                "result": self.warnings,
                "src_ip": self.key,
                "alert_timestamp": datetime.datetime.now().isoformat(),
                "suspicious_batch_id": str(self.suspicious_batch_id),
                "detector_name": self.name,
            }

            if self.produce_topics:
                logger.info(f"Producing alert to Kafka: {alert}")

                if self.kafka_produce_handler is None:
                    self.kafka_produce_handler = ExactlyOnceKafkaProduceHandler()

                for topic in self.produce_topics:
                    self.kafka_produce_handler.produce(
                        topic=topic,
                        data=json.dumps(alert),
                        key=self.key,
                    )
            else:
                logger.info("No alerter topics configured. Skipping alert output.")

            self.alerts.insert(
                dict(
                    src_ip=self.key,
                    alert_timestamp=datetime.datetime.now(),
                    suspicious_batch_id=self.suspicious_batch_id,
                    overall_score=overall_score,
                    domain_names=json.dumps(self._get_warning_requests()),
                    result=json.dumps(self.warnings),
                )
            )

            self.suspicious_batch_timestamps.insert(
                dict(
                    suspicious_batch_id=self.suspicious_batch_id,
                    src_ip=self.key,
                    stage=module_name,
                    instance_name=self.name,
                    status="finished",
                    timestamp=datetime.datetime.now(),
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
                        status="detected",
                        timestamp=datetime.datetime.now(),
                        is_active=False,
                    )
                )
        else:
            logger.info("No warning produced.")

            self.suspicious_batch_timestamps.insert(
                dict(
                    suspicious_batch_id=self.suspicious_batch_id,
                    src_ip=self.key,
                    stage=module_name,
                    instance_name=self.name,
                    status="filtered_out",
                    timestamp=datetime.datetime.now(),
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
                        timestamp=datetime.datetime.now(),
                        is_active=False,
                    )
                )

        self.batch_tree.insert(
            dict(
                batch_row_id=row_id,
                stage=module_name,
                instance_name=self.name,
                status="finished",
                timestamp=datetime.datetime.now(),
                parent_batch_row_id=self.parent_row_id,
                batch_id=self.suspicious_batch_id,
            )
        )

        if downstream_messages:
            self._send_detector_batch(row_id, downstream_messages)

        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
                stage=module_name,
                entry_type="total_loglines",
                entry_count=0,
            )
        )

    def _send_detector_batch(self, parent_row_id, messages) -> None:
        if not self.downstream_detector_topics:
            return

        logger.info(
            f"Producing detector output to Kafka topics: {self.downstream_detector_topics}"
        )
        data_to_send = {
            "batch_tree_row_id": parent_row_id,
            "batch_id": self.suspicious_batch_id,
            "begin_timestamp": self.begin_timestamp,
            "end_timestamp": self.end_timestamp,
            "data": messages,
        }
        batch_schema = marshmallow_dataclass.class_schema(Batch)()

        if self.kafka_produce_handler is None:
            self.kafka_produce_handler = ExactlyOnceKafkaProduceHandler()

        for topic in self.downstream_detector_topics:
            self.kafka_produce_handler.produce(
                topic=topic,
                data=batch_schema.dumps(data_to_send),
                key=self.key,
            )

    def _get_warning_requests(self) -> list:
        return [
            warning.get("request", warning.get("request_domain", warning))
            for warning in self.warnings
        ]

    def _get_downstream_messages(self) -> list:
        messages = [
            warning["request"] for warning in self.warnings if "request" in warning
        ]
        if messages:
            return messages
        return self.messages

    # TODO: test bootstrap!
    def bootstrap_detector_instance(self):
        """
        Main processing loop for the detector instance.

        This method implements the core processing loop that continuously:
        1. Fetches data from Kafka
        2. Performs detection on the data
        3. Sends warnings for detected threats
        4. Handles exceptions and cleanup

        The loop continues until interrupted by a keyboard interrupt (Ctrl+C),
        at which point it performs a graceful shutdown.

        Note:
            This method is designed to run in a dedicated thread or process.
        """
        while True:
            try:
                logger.debug("Before getting and filling data")
                self.get_and_fill_data()
                logger.debug("Inspect Data")
                self.detect()
                logger.debug("Send warnings")
                self.send_warning()
            except KafkaMessageFetchException as e:  # pragma: no cover
                logger.debug(e)
            except IOError as e:
                logger.error(e)
                raise e
            except ValueError as e:
                logger.debug(e)
            except KeyboardInterrupt:
                logger.info("Closing down Detector...")
                break
            finally:
                self.clear_data()

    async def start(self):  # pragma: no cover
        """
        Start the detector instance asynchronously.

        This method sets up the detector to run in an asynchronous execution context,
        allowing it to operate concurrently with other components in the system.
        """
        loop = asyncio.get_event_loop()
        executor = create_pipeline_executor(config, module_name, self.name)
        try:
            await loop.run_in_executor(executor, self.bootstrap_detector_instance)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)


async def main():  # pragma: no cover
    """
    Initialize and start all detector instances defined in the configuration.

    This function:
    1. Reads detector configurations
    2. Dynamically loads detector classes
    3. Creates detector instances
    4. Starts all detectors concurrently
    """
    # ensure all detectors configure what to do
    # instead of doing ensure alert directly we now use alerter topics

    tasks = []
    for detector_config in DETECTORS:
        consume_topic = build_detector_consume_topic(detector_config)
        produce_topics = build_alerter_topics(detector_config)
        downstream_detector_topics = build_downstream_detector_topics(detector_config)
        logger.info(
            "Detector %s configured with alerter topics %s and downstream detector topics %s",
            detector_config["name"],
            produce_topics,
            downstream_detector_topics,
        )

        class_name = detector_config["detector_class_name"]
        module_name = f"{PLUGIN_PATH}.{detector_config['detector_module_name']}"
        module = importlib.import_module(module_name)
        DetectorClass = getattr(module, class_name)
        detector = DetectorClass(
            detector_config=detector_config,
            consume_topic=consume_topic,
            produce_topics=produce_topics,
            downstream_detector_topics=downstream_detector_topics,
        )
        tasks.append(asyncio.create_task(detector.start()))
    await asyncio.gather(*tasks)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())

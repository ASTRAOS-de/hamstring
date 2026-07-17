import json
import asyncio
import datetime
import uuid
from abc import ABC, abstractmethod
import importlib
from pathlib import Path

from src.base.clickhouse_kafka_sender import ClickHouseKafkaSender
from src.base.utils import setup_config
from src.base.execution import (
    create_pipeline_executor,
    run_thread_worker_pool,
    start_pipeline_worker_replicas,
)
from src.base.kafka import (
    KafkaMessageFetchException,
    KafkaProduceRecord,
    create_pipeline_consumer,
    create_pipeline_producer,
    decode_json_record,
    ensure_topics,
)
from src.base.log_config import get_logger

module_name = "pipeline.alerter"
logger = get_logger(module_name)

config = setup_config()
CONSUME_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"][
    "detector_to_alerter"
]
ALERTING_CONFIG = config["pipeline"].get("alerting", {})
ALERTERS = ALERTING_CONFIG.get("plugins", [])
PLUGIN_PATH = "src.alerter.plugins"


class AlerterAbstractBase(ABC):
    """
    Abstract base class for all alerter implementations.
    """

    @abstractmethod
    def __init__(self, alerter_config, consume_topic, worker_id="default") -> None:
        pass

    @abstractmethod
    def process_alert(self) -> None:
        """
        Process the alert data. Subclasses can mutate self.alert_data.
        """
        pass


class AlerterBase(AlerterAbstractBase):
    """
    Base implementation for Alerters in the pipeline.

    This class handles the common logic for consuming alerts from Kafka,
    executing custom processing via plugins, and performing base actions
    like logging to a file or forwarding to an external Kafka topic.
    """

    def __init__(self, alerter_config, consume_topic, worker_id="default") -> None:
        self.name = alerter_config.get("name", "generic")
        self.consume_topic = consume_topic
        self.worker_id = worker_id or "default"
        self.alerter_config = alerter_config
        self.alert_data = None
        self.key = None
        self.source_message = None

        self.kafka_consume_handler = create_pipeline_consumer(self.consume_topic)
        self.kafka_produce_handler = create_pipeline_producer(
            stage=module_name,
            consume_topic=consume_topic,
            instance_name=self.name,
            worker_id=self.worker_id,
        )
        self.server_log_terminal_events = ClickHouseKafkaSender(
            "server_log_terminal_events"
        )

        # Base actions config
        self.log_to_file = ALERTING_CONFIG.get("log_to_file", False)
        self.log_file_path = ALERTING_CONFIG.get(
            "log_file_path", "/opt/logs/alerts.txt"
        )
        self.log_rotation_config = ALERTING_CONFIG.get("log_rotation", {})
        self.log_rotation_enabled = self.log_rotation_config.get("enabled", False)
        self.log_retention_days = self._parse_log_retention_days(
            self.log_rotation_config.get("retention_days", 7)
        )
        self._last_log_cleanup_date = None
        self.log_to_kafka = ALERTING_CONFIG.get("log_to_kafka", False)
        self.external_kafka_topic = ALERTING_CONFIG.get(
            "external_kafka_topic", "external_alerts_topic"
        )

        if self.log_to_file:
            Path(self.log_file_path).parent.mkdir(parents=True, exist_ok=True)

        if self.log_to_kafka:
            self._setup_kafka_output_topics()

    def _setup_kafka_output_topics(self):
        """
        Ensure that the external Kafka topic exists.

        Since no internal consumer subscribes to this topic, auto-creation
        via consumer polling won't happen.
        """
        ensure_topics([self.external_kafka_topic])

    @staticmethod
    def _parse_log_retention_days(retention_days) -> int | None:
        """
        Parse the configured rotated log retention period.
        """
        if retention_days is None:
            return None
        try:
            retention_days = int(retention_days)
        except (TypeError, ValueError):
            logger.warning(
                "Invalid alert log retention_days '%s'. Keeping rotated logs for 7 days.",
                retention_days,
            )
            return 7
        if retention_days < 1:
            logger.warning(
                "Invalid alert log retention_days '%s'. Keeping rotated logs for 1 day.",
                retention_days,
            )
            return 1
        return retention_days

    def _get_active_log_file_path(
        self, timestamp: datetime.datetime | None = None
    ) -> str:
        if not self.log_rotation_enabled:
            return self.log_file_path

        timestamp = timestamp or datetime.datetime.now()
        log_path = Path(self.log_file_path)
        rotated_name = f"{log_path.stem}-{timestamp:%Y-%m-%d}{log_path.suffix}"
        return str(log_path.with_name(rotated_name))

    def _cleanup_rotated_logs(self, today: datetime.date | None = None) -> None:
        if not self.log_rotation_enabled or self.log_retention_days is None:
            return

        today = today or datetime.date.today()
        if self._last_log_cleanup_date == today:
            return

        log_path = Path(self.log_file_path)
        cutoff_date = today - datetime.timedelta(days=self.log_retention_days - 1)
        for candidate in log_path.parent.glob(f"{log_path.stem}-*{log_path.suffix}"):
            log_date = self._extract_rotated_log_date(candidate)
            if log_date is None or log_date >= cutoff_date:
                continue
            try:
                candidate.unlink()
                logger.info("%s: Removed expired alert log %s", self.name, candidate)
            except OSError as e:
                logger.warning(
                    "%s: Could not remove expired alert log %s: %s",
                    self.name,
                    candidate,
                    e,
                )

        self._last_log_cleanup_date = today

    def _extract_rotated_log_date(self, log_path: Path) -> datetime.date | None:
        stem_prefix = f"{Path(self.log_file_path).stem}-"
        if not log_path.stem.startswith(stem_prefix):
            return None

        date_value = log_path.stem[len(stem_prefix) :]
        try:
            return datetime.datetime.strptime(date_value, "%Y-%m-%d").date()
        except ValueError:
            return None

    def get_and_fill_data(self) -> None:
        if self.alert_data:
            logger.warning(
                "Alerter is busy: Not consuming new messages. Wait for the Alerter to finish the current workload."
            )
            return

        self.source_message = self.kafka_consume_handler.consume_one()
        data = decode_json_record(self.source_message)
        key = self.source_message.key
        if data:
            self.alert_data = data
            self.key = key
            logger.info(f"Received alert for processing. Belongs to subnet_id {key}.")
        else:
            logger.info(f"Received empty alert message.")

    def clear_data(self) -> None:
        self.alert_data = None
        self.key = None

    def _log_to_file_action(self):
        """
        Append the current alert_data to the configured log file.
        """
        if not self.log_to_file:
            return

        active_log_file_path = self._get_active_log_file_path()
        Path(active_log_file_path).parent.mkdir(parents=True, exist_ok=True)
        self._cleanup_rotated_logs()

        logger.info(f"{self.name}: Logging alert to file {active_log_file_path}")
        try:
            with open(active_log_file_path, "a+") as f:
                json.dump(self.alert_data, f)
                f.write("\n")
        except IOError as e:
            logger.error(f"{self.name}: Error writing alert to file: {e}")
            raise

    def _build_kafka_output_records(self) -> list[KafkaProduceRecord]:
        """
        Forward the current alert_data to the external Kafka topic.
        """
        if not self.log_to_kafka:
            return []

        logger.info(
            f"{self.name}: Forwarding alert to topic {self.external_kafka_topic}"
        )
        try:
            return [
                KafkaProduceRecord(
                    topic=self.external_kafka_topic,
                    data=json.dumps(self.alert_data),
                    key=self.key,
                )
            ]
        except Exception as e:
            logger.error(f"{self.name}: Error forwarding alert: {e}")
            raise

    def _extract_server_message_ids(self) -> set[uuid.UUID]:
        server_message_ids = set()

        def visit(value):
            if isinstance(value, dict):
                if value.get("server_message_id"):
                    self._add_server_message_id(
                        server_message_ids, value["server_message_id"]
                    )
                if isinstance(value.get("server_message_ids"), list):
                    for server_message_id in value["server_message_ids"]:
                        self._add_server_message_id(
                            server_message_ids, server_message_id
                        )
                for nested_value in value.values():
                    visit(nested_value)
            elif isinstance(value, list):
                for item in value:
                    visit(item)

        visit(self.alert_data)
        return server_message_ids

    @staticmethod
    def _add_server_message_id(
        server_message_ids: set[uuid.UUID],
        server_message_id,
    ) -> None:
        try:
            server_message_ids.add(uuid.UUID(str(server_message_id)))
        except (TypeError, ValueError):
            logger.warning(
                "Ignoring non-UUID LogServer message id '%s'.", server_message_id
            )

    def _record_alerter_terminal_events(
        self, server_message_ids: set[uuid.UUID]
    ) -> None:
        if not server_message_ids:
            return
        timestamp = datetime.datetime.now()
        for server_message_id in server_message_ids:
            self.server_log_terminal_events.insert(
                dict(
                    message_id=server_message_id,
                    stage=module_name,
                    status="processed",
                    timestamp=timestamp,
                )
            )

    def bootstrap_alerter_instance(self):
        """
        Main loop for the alerter instance.
        Consumes alerts, processes them, and executes base actions.
        """
        logger.info(f"Starting {self.name} Alerter")
        while True:
            try:
                self.get_and_fill_data()
                if self.alert_data:
                    server_message_ids = self._extract_server_message_ids()
                    # 1. Process specific action
                    self.process_alert()
                    # 2. Executing Base Logging Actions
                    self._log_to_file_action()
                    records = self._build_kafka_output_records()
                    self._record_alerter_terminal_events(server_message_ids)
                    self.kafka_produce_handler.complete(
                        records,
                        consumer=self.kafka_consume_handler,
                        consumed_messages=[self.source_message],
                    )
                else:
                    self.kafka_produce_handler.complete(
                        [],
                        consumer=self.kafka_consume_handler,
                        consumed_messages=[self.source_message],
                    )

            except KafkaMessageFetchException as e:
                logger.debug(e)
            except IOError as e:
                logger.error(e)
                raise e
            except ValueError as e:
                logger.debug(e)
            except KeyboardInterrupt:
                logger.info(f" {self.consume_topic} Closing down Alerter...")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
            finally:
                self.clear_data()

    async def start(self):
        loop = asyncio.get_running_loop()
        executor = create_pipeline_executor(config, module_name, self.name)
        try:
            await loop.run_in_executor(executor, self.bootstrap_alerter_instance)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)


def build_alerter_worker(alerter_config, consume_topic, worker_id=None):
    class_name = alerter_config.get("alerter_class_name", "GenericAlerter")
    alerter_module_name = alerter_config.get("alerter_module_name", "generic_alerter")
    plugin_module_name = f"{PLUGIN_PATH}.{alerter_module_name}"
    plugin_module = importlib.import_module(plugin_module_name)
    alerter_class = getattr(plugin_module, class_name)
    return alerter_class(
        alerter_config=alerter_config,
        consume_topic=consume_topic,
        worker_id=worker_id,
    )


def run_alerter_worker_process(
    process_index,
    threads_per_process,
    alerter_config,
    consume_topic,
):
    def worker_factory(worker_id):
        return build_alerter_worker(
            alerter_config=alerter_config,
            consume_topic=consume_topic,
            worker_id=worker_id,
        )

    run_thread_worker_pool(
        worker_factory=worker_factory,
        target_name="bootstrap_alerter_instance",
        module_name=module_name,
        instance_name=alerter_config.get("name", "generic"),
        process_index=process_index,
        threads_per_process=threads_per_process,
    )


async def main():
    tasks = []

    # Setup Generic Alerter Task
    generic_topic = f"{CONSUME_TOPIC_PREFIX}-generic"
    logger.info("Initializing Generic Alerter")

    generic_config = {"name": "generic"}

    def generic_worker_factory(
        worker_id,
        generic_config=generic_config,
        generic_topic=generic_topic,
    ):
        return build_alerter_worker(
            alerter_config=generic_config,
            consume_topic=generic_topic,
            worker_id=worker_id,
        )

    tasks.append(
        asyncio.create_task(
            start_pipeline_worker_replicas(
                config=config,
                module_name=module_name,
                instance_name="generic",
                worker_factory=generic_worker_factory,
                target_name="bootstrap_alerter_instance",
                process_entrypoint=run_alerter_worker_process,
                process_args=(generic_config, generic_topic),
            )
        )
    )

    # Setup Specific Custom Alerter Tasks
    if ALERTERS:
        for alerter_config in ALERTERS:
            logger.info(f"Initializing Custom Alerter: {alerter_config['name']}")
            consume_topic = f"{CONSUME_TOPIC_PREFIX}-{alerter_config['name']}"

            def worker_factory(
                worker_id,
                alerter_config=alerter_config,
                consume_topic=consume_topic,
            ):
                return build_alerter_worker(
                    alerter_config=alerter_config,
                    consume_topic=consume_topic,
                    worker_id=worker_id,
                )

            tasks.append(
                asyncio.create_task(
                    start_pipeline_worker_replicas(
                        config=config,
                        module_name=module_name,
                        instance_name=alerter_config["name"],
                        worker_factory=worker_factory,
                        target_name="bootstrap_alerter_instance",
                        process_entrypoint=run_alerter_worker_process,
                        process_args=(alerter_config, consume_topic),
                    )
                )
            )

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())

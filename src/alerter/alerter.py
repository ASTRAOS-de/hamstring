import json
import os
import sys
import asyncio
from abc import ABC, abstractmethod
import importlib

sys.path.append(os.getcwd())
from confluent_kafka.admin import AdminClient
from src.base.utils import setup_config, ensure_directory
from src.base.execution import create_pipeline_executor
from src.base.kafka_handler import (
    ExactlyOnceKafkaConsumeHandler,
    ExactlyOnceKafkaProduceHandler,
    KafkaMessageFetchException,
    ensure_topics,
)
from src.base.log_config import get_logger

module_name = "pipeline.alerter"
logger = get_logger(module_name)

config = setup_config()
CONSUME_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"].get(
    "detector_to_alerter", "pipeline-detector_to_alerter"
)
ALERTING_CONFIG = config["pipeline"].get("alerting", {})
ALTERTERS = ALERTING_CONFIG.get("plugins", [])
PLUGIN_PATH = "src.alerter.plugins"


class AlerterAbstractBase(ABC):
    """
    Abstract base class for all alerter implementations.
    """

    @abstractmethod
    def __init__(self, alerter_config, consume_topic) -> None:
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

    def __init__(self, alerter_config, consume_topic) -> None:
        self.name = alerter_config.get("name", "generic")
        self.consume_topic = consume_topic
        self.alerter_config = alerter_config
        self.alert_data = None
        self.key = None

        self.kafka_consume_handler = ExactlyOnceKafkaConsumeHandler(self.consume_topic)

        # Base actions config
        self.log_to_file = ALERTING_CONFIG.get("log_to_file", False)
        self.log_file_path = ALERTING_CONFIG.get(
            "log_file_path", "/opt/logs/alerts.txt"
        )
        self.log_to_kafka = ALERTING_CONFIG.get("log_to_kafka", False)
        self.external_kafka_topic = ALERTING_CONFIG.get(
            "external_kafka_topic", "external_alerts_topic"
        )

        if self.log_to_file:
            ensure_directory(self.log_file_path)

        if self.log_to_kafka:
            self._setup_kafka_output_topics()

    def _setup_kafka_output_topics(self):
        """
        Ensure that the external Kafka topic exists.

        Since no internal consumer subscribes to this topic, auto-creation
        via consumer polling won't happen. We use AdminClient to ensure
        the topic exists before producing to it.
        """
        brokers = ",".join(
            [
                f"{broker['hostname']}:{broker['internal_port']}"
                for broker in config["environment"]["kafka_brokers"]
            ]
        )
        admin_client = AdminClient({"bootstrap.servers": brokers})
        try:
            ensure_topics(admin_client, [self.external_kafka_topic])
        except Exception as e:
            logger.warning(
                f"Could not auto-create topic {self.external_kafka_topic}: {e}"
            )

        self.kafka_produce_handler = ExactlyOnceKafkaProduceHandler()

    def get_and_fill_data(self) -> None:
        if self.alert_data:
            logger.warning(
                "Alerter is busy: Not consuming new messages. Wait for the Alerter to finish the current workload."
            )
            return

        key, data = self.kafka_consume_handler.consume_as_json()
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

        logger.info(f"{self.name}: Logging alert to file {self.log_file_path}")
        try:
            with open(self.log_file_path, "a+") as f:
                json.dump(self.alert_data, f)
                f.write("\n")
        except IOError as e:
            logger.error(f"{self.name}: Error writing alert to file: {e}")
            raise

    def _log_to_kafka_action(self):
        """
        Forward the current alert_data to the external Kafka topic.
        """
        if not self.log_to_kafka:
            return

        logger.info(
            f"{self.name}: Forwarding alert to topic {self.external_kafka_topic}"
        )
        try:
            self.kafka_produce_handler.produce(
                topic=self.external_kafka_topic,
                data=json.dumps(self.alert_data),
                key=self.key,
            )
        except Exception as e:
            logger.error(f"{self.name}: Error forwarding alert: {e}")
            raise

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
                    # 1. Process specific action
                    self.process_alert()
                    # 2. Executing Base Logging Actions
                    self._log_to_file_action()
                    self._log_to_kafka_action()

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


async def main():
    tasks = []

    # Setup Generic Alerter Task
    generic_topic = f"{CONSUME_TOPIC_PREFIX}-generic"
    logger.info("Initializing Generic Alerter")
    class_name = "GenericAlerter"
    mod_name = f"{PLUGIN_PATH}.generic_alerter"
    module = importlib.import_module(mod_name)
    AlerterClass = getattr(module, class_name)

    generic_alerter = AlerterClass(
        alerter_config={"name": "generic"}, consume_topic=generic_topic
    )
    tasks.append(asyncio.create_task(generic_alerter.start()))

    # Setup Specific Custom Alerter Tasks
    if ALTERTERS:
        for alerter_config in ALTERTERS:
            logger.info(f"Initializing Custom Alerter: {alerter_config['name']}")
            consume_topic = f"{CONSUME_TOPIC_PREFIX}-{alerter_config['name']}"
            class_name = alerter_config["alerter_class_name"]
            mod_name = f"{PLUGIN_PATH}.{alerter_config['alerter_module_name']}"
            module = importlib.import_module(mod_name)
            AlerterClass = getattr(module, class_name)

            alerter_instance = AlerterClass(
                alerter_config=alerter_config, consume_topic=consume_topic
            )
            tasks.append(asyncio.create_task(alerter_instance.start()))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())

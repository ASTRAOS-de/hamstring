import asyncio
import os
import sys
from dataclasses import asdict

import marshmallow_dataclass

sys.path.append(os.getcwd())
from src.monitoring.clickhouse_batch_sender import *
from src.base.kafka_handler import SimpleKafkaConsumeHandler
from src.base.data_classes.clickhouse_connectors import TABLE_NAME_TO_TYPE
from src.base.log_config import get_logger
from src.base.utils import setup_config
from src.base.execution import (
    run_thread_worker_pool,
    start_pipeline_worker_replicas,
)
from src.base.retry import load_retry_settings, retry_forever

logger = get_logger()
module_name = "monitoring.agent"

CONFIG = setup_config()
RETRY_SETTINGS = load_retry_settings(CONFIG)
CREATE_TABLES_DIRECTORY = "docker/create_tables"  # TODO: Get from config
CLICKHOUSE_HOSTNAME = CONFIG["environment"]["monitoring"]["clickhouse_server"][
    "hostname"
]
MONITORING_CONSUMER_CONFIG = CONFIG["pipeline"]["monitoring"]["kafka_consumer"]
MONITORING_CONSUMER_BATCH_SIZE = max(
    1, int(MONITORING_CONSUMER_CONFIG["batch_size"])
)
MONITORING_CONSUMER_TIMEOUT_MS = max(
    0, int(MONITORING_CONSUMER_CONFIG["timeout_ms"])
)


def prepare_all_tables():
    """Prepares and creates all ClickHouse tables from SQL files.

    Reads all SQL files from the CREATE_TABLES_DIRECTORY and executes them
    to create the required database tables for monitoring data storage.

    Raises:
        Exception: If any CREATE TABLE statement fails to execute.
    """

    def _load_contents(file_name: str) -> str:
        with open(file_name, "r") as file:
            return file.read()

    def _iter_statements(sql_content: str):
        for statement in sql_content.split(";"):
            statement = statement.strip()
            if statement:
                yield statement

    for filename in sorted(os.listdir(CREATE_TABLES_DIRECTORY)):
        if filename.endswith(".sql"):
            file_path = os.path.join(CREATE_TABLES_DIRECTORY, filename)
            sql_content = _load_contents(file_path)

            with retry_forever(
                create_clickhouse_client,
                "ClickHouse table preparation connection",
                RETRY_SETTINGS,
            ) as client:
                for statement in _iter_statements(sql_content):
                    try:
                        client.command(statement)
                    except Exception as e:
                        logger.critical("Error in CREATE TABLE statement")
                        raise e


class MonitoringAgent:
    """Main component of the Monitoring stage to collect and store pipeline data

    Consumes monitoring data from Kafka topics and batches them for efficient
    insertion into ClickHouse. Handles data deserialization and forwards it to
    the batch sender for persistent storage.
    """

    def __init__(self, worker_id: str = "default"):
        """
        Sets up consumption from all ClickHouse-related Kafka topics and
        initializes the batch sender for efficient data insertion.
        """
        self.worker_id = worker_id
        self.table_names = [
            "server_logs",
            "server_logs_timestamps",
            "server_log_to_logline",
            "server_log_terminal_events",
            "failed_loglines",
            "logline_to_batches",
            "loglines",
            "logline_timestamps",
            "batch_timestamps",
            "suspicious_batches_to_batch",
            "suspicious_batch_timestamps",
            "alerts",
            "fill_levels",
            "batch_tree",
        ]

        self.topics = [f"clickhouse_{table_name}" for table_name in self.table_names]
        self.kafka_consumer = SimpleKafkaConsumeHandler(self.topics)
        # This worker explicitly flushes before committing its Kafka offsets.
        # A timer would race with record decoding and create smaller inserts.
        self.batch_sender = ClickHouseBatchSender(use_timer=False)
        self.data_schemas = {
            table_name: marshmallow_dataclass.class_schema(
                TABLE_NAME_TO_TYPE[table_name]
            )()
            for table_name in self.table_names
        }

    def run(self) -> None:
        """Starts the monitoring agent to consume and process data continuously.

        Runs an infinite loop to consume messages from Kafka topics, deserialize
        the data according to table schemas, and forward it to the batch sender
        for insertion into ClickHouse.

        Raises:
            KeyboardInterrupt: When the agent is manually stopped.
            Exception: For any other processing errors (logged as warnings).
        """
        try:
            while True:
                try:
                    source_records = self.kafka_consumer.consume_batch(
                        MONITORING_CONSUMER_BATCH_SIZE,
                        MONITORING_CONSUMER_TIMEOUT_MS,
                    )
                    if not source_records:
                        continue

                    logger.debug(
                        "Monitoring worker %s fetched %d Kafka record(s).",
                        self.worker_id,
                        len(source_records),
                    )
                    for source_record in source_records:
                        try:
                            table_name = source_record.topic.removeprefix(
                                "clickhouse_"
                            )
                            data = self.data_schemas[table_name].loads(
                                source_record.value
                            )
                            self.batch_sender.add(table_name, asdict(data))
                        except Exception as exception:
                            logger.warning(
                                "Discarding invalid monitoring record at %s[%d] "
                                "offset %d: %s",
                                source_record.topic,
                                source_record.partition,
                                source_record.offset,
                                exception,
                            )

                    self.batch_sender.insert_all()
                    self.kafka_consumer.commit(source_records)
                except KeyboardInterrupt:
                    logger.info("Stopped MonitoringAgent.")
                    break
                except Exception as e:
                    logger.warning(e)
        finally:
            self.batch_sender.insert_all()


def build_monitoring_worker(worker_id: str) -> MonitoringAgent:
    """Create one independently consumable monitoring worker."""
    return MonitoringAgent(worker_id=worker_id)


def run_monitoring_worker_process(
    process_index: int, threads_per_process: int
) -> None:
    """Run all monitoring threads assigned to one process."""
    run_thread_worker_pool(
        worker_factory=build_monitoring_worker,
        target_name="run",
        module_name=module_name,
        instance_name=None,
        process_index=process_index,
        threads_per_process=threads_per_process,
    )


async def start_monitoring_workers() -> None:
    """Start every configured monitoring consumer replica."""
    await start_pipeline_worker_replicas(
        config=CONFIG,
        module_name=module_name,
        instance_name=None,
        worker_factory=build_monitoring_worker,
        target_name="run",
        process_entrypoint=run_monitoring_worker_process,
    )


def main():
    """Start all configured :class:`MonitoringAgent` workers.

    Each worker owns an independent Kafka consumer and ClickHouse batch sender.
    """
    asyncio.run(start_monitoring_workers())


if __name__ == "__main__":  # pragma: no cover
    main()

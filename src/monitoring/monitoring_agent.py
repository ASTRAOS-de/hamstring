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
from src.base.execution import create_pipeline_executor
from src.base.retry import retry_forever

logger = get_logger()
module_name = "monitoring.agent"

CONFIG = setup_config()
CREATE_TABLES_DIRECTORY = "docker/create_tables"  # TODO: Get from config
CLICKHOUSE_HOSTNAME = CONFIG["environment"]["monitoring"]["clickhouse_server"][
    "hostname"
]


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

            client = retry_forever(
                lambda: clickhouse_connect.get_client(host=CLICKHOUSE_HOSTNAME),
                "ClickHouse table preparation connection",
            )
            with client:
                for statement in _iter_statements(sql_content):
                    try:
                        retry_forever(
                            lambda statement=statement: client.command(statement),
                            "ClickHouse CREATE TABLE statement",
                        )
                    except Exception as e:
                        logger.critical("Error in CREATE TABLE statement")
                        raise e


class MonitoringAgent:
    """Main component of the Monitoring stage to collect and store pipeline data

    Consumes monitoring data from Kafka topics and batches them for efficient
    insertion into ClickHouse. Handles data deserialization and forwards it to
    the batch sender for persistent storage.
    """

    def __init__(self):
        """
        Sets up consumption from all ClickHouse-related Kafka topics and
        initializes the batch sender for efficient data insertion.
        """
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
        self.batch_sender = ClickHouseBatchSender()

    async def start(self):
        """Starts the monitoring agent to consume and process data continuously.

        Runs an infinite loop to consume messages from Kafka topics, deserialize
        the data according to table schemas, and forward it to the batch sender
        for insertion into ClickHouse.

        Raises:
            KeyboardInterrupt: When the agent is manually stopped.
            Exception: For any other processing errors (logged as warnings).
        """
        loop = asyncio.get_running_loop()
        executor = create_pipeline_executor(CONFIG, module_name)

        try:
            while True:
                try:
                    key, value, topic = await loop.run_in_executor(
                        executor, self.kafka_consumer.consume
                    )
                    logger.debug(f"From Kafka: {value}")

                    table_name = topic.replace("clickhouse_", "")
                    data_schema = marshmallow_dataclass.class_schema(
                        TABLE_NAME_TO_TYPE.get(table_name)
                    )()
                    data = data_schema.loads(value)

                    self.batch_sender.add(table_name, asdict(data))
                except KeyboardInterrupt:
                    logger.info("Stopped MonitoringAgent.")
                    break
                except Exception as e:
                    logger.warning(e)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)


def main():
    """Creates the :class:`MonitoringAgent` instance and starts it.

    Entry point for the monitoring agent that initializes and runs
    the asynchronous monitoring process.
    """
    clickhouse_consumer = MonitoringAgent()
    asyncio.run(clickhouse_consumer.start())


if __name__ == "__main__":  # pragma: no cover
    main()

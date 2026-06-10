import asyncio
import datetime
import os
import sys
import uuid

import aiofiles

sys.path.append(os.getcwd())
from src.base.kafka_handler import (
    ExactlyOnceKafkaConsumeHandler,
    ExactlyOnceKafkaProduceHandler,
)
from src.base.clickhouse_kafka_sender import ClickHouseKafkaSender
from src.base.utils import setup_config, get_zeek_sensor_topic_base_names
from src.base.execution import create_pipeline_executor
from src.base.log_config import get_logger

module_name = "log_storage.logserver"
logger = get_logger(module_name)

config = setup_config()
CONSUME_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"][
    "logserver_in"
]
PRODUCE_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"][
    "logserver_to_collector"
]

SENSOR_PROTOCOLS = get_zeek_sensor_topic_base_names(config)

READ_FROM_FILE = config["pipeline"]["log_storage"]["logserver"]["input_file"]
COLLECTORS = [
    collector for collector in config["pipeline"]["log_collection"]["collectors"]
]


class LogServer:
    """Main component of the Log Storage stage to enter data into the pipeline

    Receives and sends single log lines. Simultaneously, listens for messages via Kafka and reads
    newly added lines from an input file. Sends every log line to a Kafka topic under which it is obtained by
    the next stage.
    """

    def __init__(self, consume_topic, produce_topics) -> None:

        self.consume_topic = consume_topic
        self.produce_topics = produce_topics

        self.kafka_consume_handler = ExactlyOnceKafkaConsumeHandler(consume_topic)
        self.kafka_produce_handler = ExactlyOnceKafkaProduceHandler()

        # databases
        self.server_logs = ClickHouseKafkaSender("server_logs")
        self.server_logs_timestamps = ClickHouseKafkaSender("server_logs_timestamps")

    async def start(self) -> None:
        """Starts the tasks to both fetch messages from Kafka and read them from the input file."""
        logger.info(
            "LogServer started:\n"
            f"    ⤷  receiving on Kafka topic '{self.consume_topic}'\n"
            f"    ⤷  sending on Kafka topics '{self.produce_topics}'"
        )

        loop = asyncio.get_running_loop()
        executor = create_pipeline_executor(config, module_name, self.consume_topic)
        try:
            await loop.run_in_executor(executor, self.fetch_from_kafka)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
        # if awaited completely then the while True has come to an end
        logger.info("LogServer stopped.")

    def send(self, message_id: uuid.UUID, message: str) -> None:
        """Sends a message using Kafka.

        Logs the time of sending the message to Kafka as a "timestamp_out" event.

        Args:
            message_id (uuid.UUID): UUID of the message to be sent.
            message (str): Message to be sent.
        """
        for topic in self.produce_topics:
            self.kafka_produce_handler.produce(topic=topic, data=message)
            logger.debug(f"Sent: '{message}' to topic {topic}")

        self.server_logs_timestamps.insert(
            dict(
                message_id=message_id,
                event="timestamp_out",
                event_timestamp=datetime.datetime.now(),
            )
        )

    def fetch_from_kafka(self) -> None:
        """Fetches data from the configured Kafka topic in a loop.

        Starts an asynchronous loop to continuously fetch new data from the Kafka topic.
        When a message is consumed, the unprocessed log line string including
        its timestamp ("timestamp_in") is logged.
        """
        while True:
            key, value, topic = self.kafka_consume_handler.consume()
            logger.debug(f"From Kafka: '{value}'")

            message_id = uuid.uuid4()
            self.server_logs.insert(
                dict(
                    message_id=message_id,
                    timestamp_in=datetime.datetime.now(),
                    message_text=value,
                )
            )

            self.send(message_id, value)


async def main() -> None:
    """
    Creates the :class:`LogServer` instance and starts it for every topic used by any of the Zeek-sensors.
    """
    tasks = []
    for protocol in SENSOR_PROTOCOLS:
        consume_topic = f"{CONSUME_TOPIC_PREFIX}-{protocol}"
        produce_topics = [
            f'{PRODUCE_TOPIC_PREFIX}-{collector["name"]}'
            for collector in COLLECTORS
            if collector["protocol_base"] == protocol
        ]
        server_instance = LogServer(
            consume_topic=consume_topic, produce_topics=produce_topics
        )
        tasks.append(asyncio.create_task(server_instance.start()))

    await asyncio.gather(*tasks)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())

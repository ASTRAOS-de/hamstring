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
from src.base.execution import (
    create_pipeline_executor,
    run_thread_worker_pool,
    start_pipeline_worker_replicas,
)
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
        self.monitoring_kafka_producer = ClickHouseKafkaSender.create_shared_producer()

        # databases
        self.server_logs = ClickHouseKafkaSender(
            "server_logs", self.monitoring_kafka_producer
        )
        self.server_logs_timestamps = ClickHouseKafkaSender(
            "server_logs_timestamps", self.monitoring_kafka_producer
        )

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
            self.kafka_produce_handler.produce(
                topic=topic,
                data=message,
                key=str(message_id),
            )
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
            source_messages = self.kafka_consume_handler.consume_batch()
            if not source_messages:
                continue

            with self.kafka_produce_handler.transaction_batch(
                self.kafka_consume_handler, source_messages
            ):
                for source_message in source_messages:
                    value = source_message.value
                    if value is None:
                        raise ValueError("LogServer received a Kafka record without data.")
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


def build_logserver_worker(consume_topic, produce_topics, worker_id=None):
    worker = LogServer(consume_topic=consume_topic, produce_topics=produce_topics)
    worker.worker_id = worker_id
    return worker


def run_logserver_worker_process(
    process_index, threads_per_process, consume_topic, produce_topics
):
    def worker_factory(worker_id):
        return build_logserver_worker(
            consume_topic=consume_topic,
            produce_topics=produce_topics,
            worker_id=worker_id,
        )

    run_thread_worker_pool(
        worker_factory=worker_factory,
        target_name="fetch_from_kafka",
        module_name=module_name,
        instance_name=consume_topic,
        process_index=process_index,
        threads_per_process=threads_per_process,
    )


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

        def worker_factory(
            worker_id,
            consume_topic=consume_topic,
            produce_topics=produce_topics,
        ):
            return build_logserver_worker(
                consume_topic=consume_topic,
                produce_topics=produce_topics,
                worker_id=worker_id,
            )

        tasks.append(
            asyncio.create_task(
                start_pipeline_worker_replicas(
                    config=config,
                    module_name=module_name,
                    instance_name=consume_topic,
                    worker_factory=worker_factory,
                    target_name="fetch_from_kafka",
                    process_entrypoint=run_logserver_worker_process,
                    process_args=(consume_topic, produce_topics),
                )
            )
        )

    await asyncio.gather(*tasks)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())

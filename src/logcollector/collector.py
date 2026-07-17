import asyncio
import datetime
import ipaddress
import json
import time
import uuid

import marshmallow_dataclass

from src.base.clickhouse_kafka_sender import ClickHouseKafkaSender
from src.base.data_classes.batch import Batch
from src.base.kafka import (
    KAFKA_SETTINGS,
    KafkaProduceRecord,
    create_pipeline_consumer,
    create_pipeline_producer,
)
from src.base.pipeline_routing import server_message_id_from_headers
from src.base.logline_handler import LoglineHandler
from src.base import utils
from src.base.execution import (
    create_pipeline_executor,
    run_thread_worker_pool,
    start_pipeline_worker_replicas,
)
from src.base.log_config import get_logger
from src.logcollector.batch_handler import BatchAccumulator

module_name = "log_collection.collector"
logger = get_logger(module_name)

config = utils.setup_config()

REQUIRED_FIELDS = [
    "ts",
    "src_ip",
]
PRODUCE_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"][
    "batch_sender_to_prefilter"
]
CONSUME_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"][
    "logserver_to_collector"
]

SENSOR_PROTOCOLS = utils.get_zeek_sensor_topic_base_names(config)
PREFILTERS = config["pipeline"]["log_filtering"]

COLLECTORS = [
    collector for collector in config["pipeline"]["log_collection"]["collectors"]
]


class LogCollector:
    """Main component of the Log Collection stage to pre-process and format data

    Consumes incoming loglines from the LogServer. Validates all data fields by type and
    value, invalid loglines are discarded. All valid loglines are sent to the BatchSender.
    """

    def __init__(
        self,
        collector_name,
        protocol,
        consume_topic,
        produce_topics,
        validation_config,
        worker_id="default",
    ) -> None:
        """Initializes a new LogCollector instance with the specified configuration.

        Args:
            collector_name (str): Name of the collector instance
            protocol (str): Protocol type of the log lines (e.g., 'dns', 'http')
            consume_topic (str): Kafka topic to consume log lines from
            produce_topics (list[str]): List of Kafka topics to produce validated log lines to
            validation_config (list): Configuration for validating log line fields
        """
        self.collector_name = collector_name
        self.protocol = protocol
        self.consume_topic = consume_topic
        self.worker_id = worker_id or "default"
        self.kafka_consume_handler = create_pipeline_consumer(consume_topic)
        self.batch_configuration = utils.get_batch_configuration(collector_name)
        self.monitoring_kafka_producer = ClickHouseKafkaSender.create_shared_producer()
        self.produce_topics = produce_topics
        self.batch_handler = BatchAccumulator(
            collector_name=collector_name,
            monitoring_kafka_producer=self.monitoring_kafka_producer,
        )
        self.kafka_produce_handler = create_pipeline_producer(
            stage=module_name,
            consume_topic=consume_topic,
            instance_name=collector_name,
            worker_id=self.worker_id,
        )
        self.pending_source_records = []
        self.pending_batch_started_at = None
        self.poll_timeout_ms = max(1, int(self.batch_configuration["poll_timeout_ms"]))
        self.max_kafka_record_bytes = KAFKA_SETTINGS.max_record_bytes
        if self.max_kafka_record_bytes <= 0:
            raise ValueError("max_kafka_record_bytes must be greater than zero.")
        self.logline_handler = LoglineHandler(validation_config)

        # databases
        self.failed_protocol_loglines = ClickHouseKafkaSender(
            "failed_loglines", self.monitoring_kafka_producer
        )
        self.protocol_loglines = ClickHouseKafkaSender(
            "loglines", self.monitoring_kafka_producer
        )
        self.logline_timestamps = ClickHouseKafkaSender(
            "logline_timestamps", self.monitoring_kafka_producer
        )
        self.server_log_to_logline = ClickHouseKafkaSender(
            "server_log_to_logline", self.monitoring_kafka_producer
        )
        self.server_log_terminal_events = ClickHouseKafkaSender(
            "server_log_terminal_events", self.monitoring_kafka_producer
        )
        self.fill_levels = ClickHouseKafkaSender(
            "fill_levels", self.monitoring_kafka_producer
        )

        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
                stage=module_name,
                entry_type="total_loglines",
                entry_count=0,
            )
        )

    async def start(self) -> None:
        """Starts the LogCollector processing loop.

        This method initializes the Kafka message fetching process and runs it in an executor
        to avoid blocking the asyncio event loop. It logs the startup information and
        continues processing until interrupted.

        """
        logger.info(
            "LogCollector started:\n"
            f"    ⤷  receiving on Kafka topic '{self.consume_topic}'"
        )
        loop = asyncio.get_event_loop()
        executor = create_pipeline_executor(config, module_name, self.collector_name)
        try:
            await loop.run_in_executor(executor, self.fetch)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        logger.info("LogCollector stopped.")

    def fetch(self) -> None:
        """Continuously listens for messages on the configured Kafka topic.

        This method runs in an infinite loop, consuming messages from Kafka and
        processing them through the send method. It blocks until messages are
        available on the Kafka topic.

        Note:
            This method is intended to be run in a separate thread via run_in_executor
            since it contains a blocking loop.
        """

        while True:
            source_records = self.kafka_consume_handler.consume_batch(
                max_messages=self.batch_configuration["batch_size"],
                timeout_ms=self.poll_timeout_ms,
            )
            for source_record in source_records:
                if source_record.value is None:
                    raise ValueError(
                        "Logcollector received Kafka record without a value."
                    )
                if not self.pending_source_records:
                    self.pending_batch_started_at = time.monotonic()
                self.pending_source_records.append(source_record)
                logger.debug(f"From Kafka: '{source_record.value}'")
                result = self.send(
                    datetime.datetime.now(),
                    source_record.value,
                    server_message_id=server_message_id_from_headers(
                        source_record.headers
                    ),
                )
                if result is not None:
                    subnet_id, message = result
                    batch_size = self.batch_handler.add_message(subnet_id, message)
                    if batch_size >= self.batch_configuration["batch_size"]:
                        self._flush_pending_batches()

            if self._pending_batch_timed_out():
                self._flush_pending_batches()

    def send(
        self,
        timestamp_in: datetime.datetime,
        message: str,
        server_message_id: str | uuid.UUID | None = None,
    ) -> tuple[str, str] | None:
        """Processes and sends a log line to the batch handler after validation.

        This method:
        1. Validates the log line format and required fields
        2. Stores valid log lines in the database
        3. Calculates the subnet ID for batch processing
        4. Adds the log line to the batch handler

        Args:
            timestamp_in (datetime.datetime): Timestamp when the log line entered the pipeline
            message (str): Raw log line message in JSON format
            server_message_id (str | uuid.UUID | None): Optional LogServer message id
                received as the Kafka key.
        """
        server_message_uuid = self._parse_server_message_id(server_message_id)
        try:
            fields = self.logline_handler.validate_logline_and_get_fields_as_json(
                message
            )
        except ValueError:
            timestamp_failed = datetime.datetime.now()
            self.failed_protocol_loglines.insert(
                dict(
                    message_text=message,
                    timestamp_in=timestamp_in,
                    timestamp_failed=timestamp_failed,
                    reason_for_failure=None,  # TODO: Add actual reason
                )
            )
            if server_message_uuid:
                self.server_log_terminal_events.insert(
                    dict(
                        message_id=server_message_uuid,
                        stage=module_name,
                        status="failed",
                        timestamp=timestamp_failed,
                    )
                )
            return
        additional_fields = fields.copy()
        for field in REQUIRED_FIELDS:
            additional_fields.pop(field)
        subnet_id = self._get_subnet_id(ipaddress.ip_address(fields.get("src_ip")))
        logline_id = uuid.uuid4()
        self.protocol_loglines.insert(
            dict(
                logline_id=logline_id,
                subnet_id=subnet_id,
                timestamp=datetime.datetime.fromisoformat(fields.get("ts")),
                src_ip=fields.get("src_ip"),
                additional_fields=json.dumps(additional_fields),
            )
        )
        if server_message_uuid:
            self.server_log_to_logline.insert(
                dict(
                    timestamp=datetime.datetime.now(),
                    message_id=server_message_uuid,
                    logline_id=logline_id,
                )
            )
        self.logline_timestamps.insert(
            dict(
                logline_id=logline_id,
                stage=module_name,
                status="in_process",
                timestamp=timestamp_in,
                is_active=True,
            )
        )
        message_fields = fields.copy()
        message_fields["logline_id"] = str(logline_id)
        if server_message_uuid:
            message_fields["server_message_id"] = str(server_message_uuid)

        self.logline_timestamps.insert(
            dict(
                logline_id=logline_id,
                stage=module_name,
                status="finished",
                timestamp=datetime.datetime.now(),
                is_active=True,
            )
        )
        message = json.dumps(message_fields)
        logger.debug(f"Sent: {message}")
        return subnet_id, message

    def _pending_batch_timed_out(self) -> bool:
        """Return whether the configured batch timeout elapsed."""
        return (
            self.pending_batch_started_at is not None
            and time.monotonic() - self.pending_batch_started_at
            >= self.batch_configuration["batch_timeout"]
        )

    def _flush_pending_batches(self) -> None:
        """Publish completed batches and their source offsets atomically."""
        if not self.pending_source_records:
            return

        schema = marshmallow_dataclass.class_schema(Batch)()
        records = []
        for subnet_id, data in self.batch_handler.complete_all():
            for payload in self._serialize_batch_packets(data, schema):
                records.extend(
                    KafkaProduceRecord(topic=topic, data=payload, key=subnet_id)
                    for topic in self.produce_topics
                )

        self.kafka_produce_handler.complete(
            records,
            consumer=self.kafka_consume_handler,
            consumed_messages=self.pending_source_records,
        )
        self.pending_source_records.clear()
        self.pending_batch_started_at = None

    def _serialize_batch_packets(self, data: dict, schema) -> list[str]:
        """Serialize one logical batch into broker-safe Kafka record payloads.

        The logical collector batch may be larger than one Kafka record. Keep
        its batch identity and split only its ``data`` list into independently
        consumable records. Candidate payloads are serialized before measuring
        them so JSON escaping and schema formatting cannot invalidate the limit.
        """
        messages = data["data"]
        packets = []
        current_messages = []
        current_payload = None
        framing_reserve = min(16_384, max(1, self.max_kafka_record_bytes // 10))
        payload_limit = self.max_kafka_record_bytes - framing_reserve

        for message in messages:
            candidate_messages = [*current_messages, message]
            candidate_payload = schema.dumps(
                {**data, "data": candidate_messages}
            )
            if len(candidate_payload.encode("utf-8")) <= payload_limit:
                current_messages = candidate_messages
                current_payload = candidate_payload
                continue

            if current_payload is not None:
                packets.append(current_payload)

            single_message_payload = schema.dumps({**data, "data": [message]})
            single_message_size = len(single_message_payload.encode("utf-8"))
            if single_message_size > payload_limit:
                raise ValueError(
                    "One serialized logline packet exceeds the Kafka payload limit "
                    f"({single_message_size}/{payload_limit} bytes). Reduce the "
                    "logline size or increase KAFKA_MAX_RECORD_BYTES together with "
                    "KAFKA_BROKER_MAX_RECORD_BYTES."
                )
            current_messages = [message]
            current_payload = single_message_payload

        if current_payload is not None:
            packets.append(current_payload)
        return packets

    @staticmethod
    def _parse_server_message_id(
        server_message_id: str | uuid.UUID | None,
    ) -> uuid.UUID | None:
        if not server_message_id:
            return None
        if isinstance(server_message_id, uuid.UUID):
            return server_message_id
        try:
            return uuid.UUID(str(server_message_id))
        except (TypeError, ValueError):
            logger.warning(
                "Ignoring non-UUID LogServer message id '%s'.", server_message_id
            )
            return None

    def _get_subnet_id(
        self, address: ipaddress.IPv4Address | ipaddress.IPv6Address
    ) -> str:
        """Calculates the subnet ID for an IP address based on batch configuration.

        This method normalizes the IP address to the configured subnet prefix length
        and returns a string representation of the subnet.

        Args:
            address (ipaddress.IPv4Address | ipaddress.IPv6Address): IP address to process

        Returns:
            str: Subnet ID in the format "network_address/prefix_length"
                Example: "192.168.1.0_24" or "2001:db8::/64"

        Raises:
            ValueError: If the address is neither IPv4 nor IPv6 address type

        """
        if not isinstance(
            address, (ipaddress.IPv4Address, ipaddress.IPv6Address)
        ):
            raise ValueError("Unsupported IP address type")

        address_family = "ipv4" if address.version == 4 else "ipv6"
        prefix_length = self.batch_configuration["subnet_id"][
            f"{address_family}_prefix_length"
        ]
        network = ipaddress.ip_network((address, prefix_length), strict=False)
        return f"{network.network_address}_{network.prefixlen}"


def build_logcollector_worker(
    collector_name,
    protocol,
    consume_topic,
    produce_topics,
    validation_config,
    worker_id=None,
):
    worker = LogCollector(
        collector_name=collector_name,
        protocol=protocol,
        consume_topic=consume_topic,
        produce_topics=produce_topics,
        validation_config=validation_config,
        worker_id=worker_id,
    )
    return worker


def run_logcollector_worker_process(
    process_index,
    threads_per_process,
    collector_name,
    protocol,
    consume_topic,
    produce_topics,
    validation_config,
):
    def worker_factory(worker_id):
        return build_logcollector_worker(
            collector_name=collector_name,
            protocol=protocol,
            consume_topic=consume_topic,
            produce_topics=produce_topics,
            validation_config=validation_config,
            worker_id=worker_id,
        )

    run_thread_worker_pool(
        worker_factory=worker_factory,
        target_name="fetch",
        module_name=module_name,
        instance_name=collector_name,
        process_index=process_index,
        threads_per_process=threads_per_process,
    )


async def main() -> None:
    """Creates and starts all configured LogCollector instances.

    This function:
    1. Iterates through all collectors defined in the configuration
    2. Creates a LogCollector instance for each collector
    3. Starts each collector in its own asyncio task
    4. Waits for all collectors to complete (which is effectively forever)

    """
    tasks = []

    for collector in COLLECTORS:
        protocol = collector["protocol_base"]
        consume_topic = f"{CONSUME_TOPIC_PREFIX}-{collector['name']}"
        produce_topics = [
            f"{PRODUCE_TOPIC_PREFIX}-{prefilter['name']}"
            for prefilter in PREFILTERS
            if collector["name"] == prefilter["collector_name"]
        ]
        validation_config = collector["required_log_information"]

        def worker_factory(
            worker_id,
            collector=collector,
            protocol=protocol,
            consume_topic=consume_topic,
            produce_topics=produce_topics,
            validation_config=validation_config,
        ):
            return build_logcollector_worker(
                collector_name=collector["name"],
                protocol=protocol,
                consume_topic=consume_topic,
                produce_topics=produce_topics,
                validation_config=validation_config,
                worker_id=worker_id,
            )

        tasks.append(
            asyncio.create_task(
                start_pipeline_worker_replicas(
                    config=config,
                    module_name=module_name,
                    instance_name=collector["name"],
                    worker_factory=worker_factory,
                    target_name="fetch",
                    process_entrypoint=run_logcollector_worker_process,
                    process_args=(
                        collector["name"],
                        protocol,
                        consume_topic,
                        produce_topics,
                        validation_config,
                    ),
                )
            )
        )
    await asyncio.gather(*tasks)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())

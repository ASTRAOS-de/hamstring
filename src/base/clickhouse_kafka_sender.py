"""
The ClickHouseKafkaSender serves as the sender for all inserts into ClickHouse. Whenever a class wants to insert
into a ClickHouse table, the ClickHouseKafkaSender is used to send the respective insert via Kafka.
"""


import marshmallow_dataclass

from src.base.data_classes.clickhouse_connectors import TABLE_NAME_TO_TYPE
from src.base.kafka import (
    BufferedKafkaProduceHandler,
    KafkaProduceRecord,
)
from src.base.log_config import get_logger

logger = get_logger()


class ClickHouseKafkaSender:
    """Sends insert operations for the specified table via Kafka to the MonitoringAgent.

    The ClickHouseKafkaSender serves as a Kafka producer that encapsulates database insert
    operations into Kafka messages. It automatically handles data schema validation and
    serialization for the specified ClickHouse table.
    """

    @staticmethod
    def create_shared_producer() -> BufferedKafkaProduceHandler:
        """Create the non-blocking producer shared by one pipeline worker."""
        return BufferedKafkaProduceHandler()

    def __init__(
        self,
        table_name: str,
        kafka_producer: BufferedKafkaProduceHandler | None = None,
    ):
        """
        Args:
            table_name (str): Name of the ClickHouse table to send insert operations for.

        Raises:
            KeyError: If the specified table name is not found in TABLE_NAME_TO_TYPE mapping.
        """
        self.table_name = table_name
        # Monitoring has much higher fan-out than the pipeline's durable data
        # path.  Use the buffered producer so an acknowledgement for one
        # monitoring row does not stall consumption of the next logline.
        self.kafka_producer = kafka_producer or BufferedKafkaProduceHandler()
        self.data_schema = marshmallow_dataclass.class_schema(
            TABLE_NAME_TO_TYPE.get(table_name)
        )()

    def insert(self, data: dict):
        """Produces the insert operation to Kafka for ClickHouse insertion.

        Validates the provided data against the table schema, serializes it, and sends
        it to the appropriate Kafka topic for processing by the MonitoringAgent.

        Args:
            data (dict): Dictionary containing the data to insert into ClickHouse.
                         Must conform to the table's schema structure.

        Raises:
            marshmallow.ValidationError: If the data does not conform to the table schema.
            KafkaException: If the Kafka message cannot be produced.
        """
        self.kafka_producer.publish(
            [
                KafkaProduceRecord(
                    topic=f"clickhouse_{self.table_name}",
                    data=self.data_schema.dumps(data),
                )
            ]
        )

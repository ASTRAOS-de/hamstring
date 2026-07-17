import datetime
import os
import uuid
from dataclasses import dataclass
from typing import Any, Optional, get_origin

import clickhouse_connect
from clickhouse_connect.driver.exceptions import (
    InterfaceError,
    OperationalError,
    StreamClosedError,
    StreamFailureError,
)

from src.base.log_config import get_logger
from src.base.retry import load_retry_settings, retry_forever
from src.base.utils import setup_config

logger = get_logger()

CONFIG = setup_config()
RETRY_SETTINGS = load_retry_settings(CONFIG)
CLICKHOUSE_HOSTNAME = CONFIG["environment"]["monitoring"]["clickhouse_server"][
    "hostname"
]
CLICKHOUSE_USERNAME = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "hamstring")
BATCH_SIZE = CONFIG["pipeline"]["monitoring"]["clickhouse_connector"]["batch_size"]

CLICKHOUSE_RETRYABLE_EXCEPTIONS = (
    InterfaceError,
    OperationalError,
    StreamClosedError,
    StreamFailureError,
    OSError,
)


def create_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOSTNAME,
        username=CLICKHOUSE_USERNAME,
        password=CLICKHOUSE_PASSWORD,
    )


@dataclass
class Table:
    """Defines the table name and allowed column fields with types.

    Stores metadata about ClickHouse table structure including column names
    and their expected data types for validation during batch insertion.
    """

    name: str
    columns: dict[str, type]

    def verify(self, data: dict[str, Any]):
        """Verifies if the data has the correct columns and types.

        Validates that the provided data dictionary contains the expected columns
        with correct data types according to the table schema definition.

        Args:
            data (dict): The values for each cell.

        Raises:
            ValueError: If column count or column names don't match expected schema.
            TypeError: If data types don't match expected column types.
        """
        if len(data) != len(self.columns):
            raise ValueError(
                f"Wrong number of fields in data: Expected {len(self.columns)}, got {len(data)}"
            )

        for e in data:
            if e not in self.columns:
                raise ValueError(f"Wrong column name: Expected one of {self.columns}")

            expected_type = self.columns.get(e)
            value = data.get(e)

            if value is not None and not isinstance(value, expected_type):
                origin = get_origin(expected_type)
                if origin is not None and not isinstance(value, origin):
                    raise TypeError(
                        f"Column '{e}' expected type {expected_type}, got {type(value)}"
                    )
                elif origin is None:
                    raise TypeError(
                        f"Column '{e}' expected type {expected_type}, got {type(value)}"
                    )


class ClickHouseBatchSender:
    """Manages batched insert operations for ClickHouse tables.

    Collects insert commands in batches and sends them to ClickHouse when either
    the batch size limit is reached or a timeout occurs. Provides efficient bulk
    insertion with automatic schema validation for all monitored tables.
    """

    def __init__(self):
        self.tables = {
            "server_logs": Table(
                "server_logs",
                {
                    "message_id": uuid.UUID,
                    "timestamp_in": datetime.datetime,
                    "message_text": str,
                },
            ),
            "server_logs_timestamps": Table(
                "server_logs_timestamps",
                {
                    "message_id": uuid.UUID,
                    "event": str,
                    "event_timestamp": datetime.datetime,
                },
            ),
            "server_log_to_logline": Table(
                "server_log_to_logline",
                {
                    "message_id": uuid.UUID,
                    "logline_id": uuid.UUID,
                },
            ),
            "server_log_terminal_events": Table(
                "server_log_terminal_events",
                {
                    "message_id": uuid.UUID,
                    "stage": str,
                    "status": str,
                    "timestamp": datetime.datetime,
                },
            ),
            "failed_loglines": Table(
                "failed_loglines",
                {
                    "message_text": str,
                    "timestamp_in": datetime.datetime,
                    "timestamp_failed": datetime.datetime,
                    "reason_for_failure": Optional[str],
                },
            ),
            "logline_to_batches": Table(
                "logline_to_batches",
                {
                    "logline_id": uuid.UUID,
                    "batch_id": uuid.UUID,
                },
            ),
            "loglines": Table(
                "loglines",
                {
                    "logline_id": uuid.UUID,
                    "subnet_id": str,
                    "timestamp": datetime.datetime,
                    "src_ip": str,
                    "additional_fields": Optional[str],
                },
            ),
            "logline_timestamps": Table(
                "logline_timestamps",
                {
                    "logline_id": uuid.UUID,
                    "stage": str,
                    "status": str,
                    "timestamp": datetime.datetime,
                    "is_active": bool,
                },
            ),
            "batch_timestamps": Table(
                "batch_timestamps",
                {
                    "batch_id": uuid.UUID,
                    "instance_name": str,
                    "stage": str,
                    "status": str,
                    "timestamp": datetime.datetime,
                    "is_active": bool,
                    "message_count": int,
                },
            ),
            "suspicious_batches_to_batch": Table(
                "suspicious_batches_to_batch",
                {
                    "suspicious_batch_id": uuid.UUID,
                    "batch_id": uuid.UUID,
                },
            ),
            "suspicious_batch_timestamps": Table(
                "suspicious_batch_timestamps",
                {
                    "suspicious_batch_id": uuid.UUID,
                    "src_ip": str,
                    "instance_name": str,
                    "stage": str,
                    "status": str,
                    "timestamp": datetime.datetime,
                    "is_active": bool,
                    "message_count": int,
                },
            ),
            "alerts": Table(
                "alerts",
                {
                    "src_ip": str,
                    "suspicious_batch_id": uuid.UUID,
                    "alert_timestamp": datetime.datetime,
                    "overall_score": float,
                    "domain_names": str,
                    "result": str,
                },
            ),
            "fill_levels": Table(
                "fill_levels",
                {
                    "timestamp": datetime.datetime,
                    "stage": str,
                    "entry_type": str,
                    "entry_count": int,
                },
            ),
            "batch_tree": Table(
                "batch_tree",
                {
                    "batch_row_id": str,
                    "batch_id": uuid.UUID,
                    "parent_batch_row_id": str,
                    "instance_name": str,
                    "stage": str,
                    "status": str,
                    "timestamp": datetime.datetime,
                },
            ),
        }

        self.max_batch_size = BATCH_SIZE

        self.batch = {key: [] for key in self.tables}
        self._client = self._connect_client()

    def _connect_client(self):
        return retry_forever(
            create_clickhouse_client,
            "ClickHouse client connection",
            RETRY_SETTINGS,
            retryable=CLICKHOUSE_RETRYABLE_EXCEPTIONS,
        )

    def _reset_client(self) -> None:
        try:
            if self._client:
                self._client.close()
        except Exception as exception:
            logger.warning(
                "Ignoring ClickHouse client close failure during reconnect: %s",
                exception,
            )
        self._client = self._connect_client()

    def add(self, table_name: str, data: dict[str, Any]):
        """Adds the data to the batch for the table.

        Verifies the data fields first, then adds the data to the appropriate
        table batch. Triggers immediate insertion if batch size limit is reached.

        Args:
            table_name (str): Name of the table to add data to.
            data (dict): The values for each cell in the table.

        Raises:
            ValueError: If table name is invalid or data format is incorrect.
            TypeError: If data types don't match table schema.
        """
        if table_name == "batch_tree" and data.get("parent_batch_row_id") is None:
            data["parent_batch_row_id"] = ""

        self.tables.get(table_name).verify(data)
        self.batch.get(table_name).append(list(data.values()))

        if len(self.batch.get(table_name)) >= self.max_batch_size:
            self.insert(table_name)

    def insert(self, table_name: str):
        """Inserts the batch for the given table.

        Executes the accumulated batch insert operation for the specified table
        and clears the batch after successful insertion.

        Args:
            table_name (str): Name of the table to insert data to.
        """
        if self.batch[table_name]:
            pending_rows = self.batch.get(table_name)
            column_names = list(self.tables.get(table_name).columns)

            def insert_batch():
                try:
                    self._client.insert(
                        table_name,
                        pending_rows,
                        column_names=column_names,
                    )
                except CLICKHOUSE_RETRYABLE_EXCEPTIONS as exception:
                    logger.warning(
                        "ClickHouse insert for table '%s' failed, reconnecting: %s",
                        table_name,
                        exception,
                    )
                    self._reset_client()
                    raise

            retry_forever(
                insert_batch,
                f"ClickHouse insert for table '{table_name}'",
                RETRY_SETTINGS,
                retryable=CLICKHOUSE_RETRYABLE_EXCEPTIONS,
            )
            logger.debug(f"Inserted {table_name=},{pending_rows=},{column_names=}")
            self.batch[table_name] = []

    def insert_all(self):
        """Inserts the batch for every table.

        Executes batch insert operations for all tables with pending data.
        """
        for table in self.batch:
            self.insert(table)

    def close(self) -> None:
        self.insert_all()
        self._client.close()

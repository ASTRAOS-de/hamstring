"""Pure decoding functions for pipeline Kafka records."""

import json

import marshmallow_dataclass

from src.base.data_classes.batch import Batch
from src.base.kafka.records import ConsumedKafkaMessage


def decode_json_record(record: ConsumedKafkaMessage) -> dict:
    """Decode one JSON-object Kafka record."""
    if not record.value:
        return {}
    try:
        data = json.loads(record.value)
    except (TypeError, json.JSONDecodeError) as exception:
        raise ValueError("Unknown data format") from exception
    if not isinstance(data, dict):
        raise ValueError("Unknown data format")
    return data


def decode_batch_record(record: ConsumedKafkaMessage) -> Batch:
    """Decode one pipeline Batch Kafka record."""
    data = decode_json_record(record)
    if not data:
        raise ValueError("Cannot decode an empty Kafka record as Batch.")

    batch_data = data.get("data")
    if batch_data is None:
        data["data"] = []
    elif not isinstance(batch_data, list):
        raise ValueError("Batch data must be a list.")
    else:
        data["data"] = [
            json.loads(item) if isinstance(item, str) else item for item in batch_data
        ]
        if not all(isinstance(item, (dict, list)) for item in data["data"]):
            raise ValueError("Batch data contains unsupported item type.")

    batch = marshmallow_dataclass.class_schema(Batch)().load(data)
    if not isinstance(batch, Batch):
        raise ValueError("Unknown data format.")
    return batch

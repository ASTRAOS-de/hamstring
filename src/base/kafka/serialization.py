"""Deserialization behavior shared by Kafka consumer implementations."""

import json
from typing import Optional

import marshmallow_dataclass

from src.base.data_classes.batch import Batch
from src.base.kafka.records import ConsumedKafkaMessage


class KafkaSerializationMixin:
    """Decode consumed Kafka values while leaving transport logic separate."""

    def consume_as_json(
        self, source_message: ConsumedKafkaMessage | None = None
    ) -> tuple[Optional[str], dict]:
        if source_message is None:
            key, value, _topic = self.consume()
        else:
            key, value = source_message.key, source_message.value

        if not key and not value:
            return None, {}

        try:
            decoded_data = json.loads(value)
        except Exception as exception:
            raise ValueError("Unknown data format") from exception
        if not isinstance(decoded_data, dict):
            raise ValueError("Unknown data format")
        return key, decoded_data

    @staticmethod
    def _is_dicts(obj):
        return isinstance(obj, list) and all(isinstance(item, dict) for item in obj)

    @staticmethod
    def _decode_batch_data(data):
        if data is None:
            return []
        if not isinstance(data, list):
            raise ValueError("Batch data must be a list.")

        decoded_data = []
        for item in data:
            if isinstance(item, str):
                decoded_data.append(json.loads(item))
            elif isinstance(item, (dict, list)):
                decoded_data.append(item)
            else:
                raise ValueError("Batch data contains unsupported item type.")
        return decoded_data

    def consume_as_object(
        self, source_message: ConsumedKafkaMessage | None = None
    ) -> tuple[None | str, Batch]:
        if source_message is None:
            key, value, _topic = self.consume()
        else:
            key, value = source_message.key, source_message.value

        if not key and not value:
            return None, {}

        decoded_data: dict = json.loads(value)
        decoded_data["data"] = self._decode_batch_data(decoded_data.get("data"))
        batch_schema = marshmallow_dataclass.class_schema(Batch)()
        batch = batch_schema.load(decoded_data)
        if isinstance(batch, Batch):
            return key, batch
        raise ValueError("Unknown data format.")

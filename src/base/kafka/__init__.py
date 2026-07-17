"""Public Kafka API for HAMSTRING stages.

Stages use this module instead of depending on confluent-kafka or a concrete
delivery implementation. The class hierarchy and infrastructure details live
in focused submodules.
"""

from src.base.kafka.config import KAFKA_SETTINGS, KafkaSettings
from src.base.kafka.consumer import (
    ExactlyOnceKafkaConsumeHandler,
    KafkaConsumeHandler,
    KafkaMessageFetchException,
    SimpleKafkaConsumeHandler,
)
from src.base.kafka.factory import (
    create_pipeline_consumer,
    create_pipeline_producer,
)
from src.base.kafka.producer import (
    BufferedKafkaProduceHandler,
    ExactlyOnceKafkaProduceHandler,
    KafkaProduceHandler,
    SimpleKafkaProduceHandler,
)
from src.base.kafka.records import ConsumedKafkaMessage, KafkaProduceRecord
from src.base.kafka.serialization import decode_batch_record, decode_json_record
from src.base.kafka.topics import (
    KafkaTopicManager,
    build_consumer_group_id,
    build_transactional_id,
    ensure_topics,
)

__all__ = [
    "BufferedKafkaProduceHandler",
    "ConsumedKafkaMessage",
    "ExactlyOnceKafkaConsumeHandler",
    "ExactlyOnceKafkaProduceHandler",
    "KAFKA_SETTINGS",
    "KafkaConsumeHandler",
    "KafkaMessageFetchException",
    "KafkaProduceHandler",
    "KafkaProduceRecord",
    "KafkaSettings",
    "KafkaTopicManager",
    "SimpleKafkaConsumeHandler",
    "SimpleKafkaProduceHandler",
    "build_consumer_group_id",
    "build_transactional_id",
    "create_pipeline_consumer",
    "create_pipeline_producer",
    "decode_batch_record",
    "decode_json_record",
    "ensure_topics",
]

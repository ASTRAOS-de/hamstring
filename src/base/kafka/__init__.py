"""Public Kafka implementation package."""

from src.base.kafka.client import KafkaHandler
from src.base.kafka.consumer import (
    ExactlyOnceKafkaConsumeHandler,
    KafkaConsumeHandler,
    SimpleKafkaConsumeHandler,
)
from src.base.kafka.errors import (
    KafkaMessageFetchException,
    TooManyFailedAttemptsError,
)
from src.base.kafka.producer import (
    BufferedKafkaProduceHandler,
    ExactlyOnceKafkaProduceHandler,
    KafkaProduceHandler,
    SimpleKafkaProduceHandler,
)
from src.base.kafka.records import ConsumedKafkaMessage, KafkaProduceRecord
from src.base.kafka.topics import (
    KafkaTopicManager,
    build_consumer_group_id,
    ensure_topics,
)

__all__ = [
    "BufferedKafkaProduceHandler",
    "ConsumedKafkaMessage",
    "ExactlyOnceKafkaConsumeHandler",
    "ExactlyOnceKafkaProduceHandler",
    "KafkaConsumeHandler",
    "KafkaHandler",
    "KafkaMessageFetchException",
    "KafkaProduceHandler",
    "KafkaProduceRecord",
    "KafkaTopicManager",
    "SimpleKafkaConsumeHandler",
    "SimpleKafkaProduceHandler",
    "TooManyFailedAttemptsError",
    "build_consumer_group_id",
    "ensure_topics",
]

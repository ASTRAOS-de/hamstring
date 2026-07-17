"""Delivery-mode factories used by every pipeline stage."""

from src.base.kafka.config import KAFKA_SETTINGS
from src.base.kafka.consumer import (
    ExactlyOnceKafkaConsumeHandler,
    KafkaConsumeHandler,
    SimpleKafkaConsumeHandler,
)
from src.base.kafka.producer import (
    ExactlyOnceKafkaProduceHandler,
    KafkaProduceHandler,
    SimpleKafkaProduceHandler,
)
from src.base.kafka.topics import build_transactional_id


def create_pipeline_consumer(
    topics: str | list[str], mode: str | None = None
) -> KafkaConsumeHandler:
    """Create a consumer for the selected pipeline delivery mode."""
    consumer_type = (
        ExactlyOnceKafkaConsumeHandler
        if _pipeline_mode(mode) == "exactly_once"
        else SimpleKafkaConsumeHandler
    )
    return consumer_type(topics)


def create_pipeline_producer(
    stage: str,
    consume_topic: str,
    instance_name: str | None = None,
    worker_id: str | None = None,
    mode: str | None = None,
) -> KafkaProduceHandler:
    """Create a producer with the common pipeline completion interface."""
    if _pipeline_mode(mode) == "simple":
        return SimpleKafkaProduceHandler()
    return ExactlyOnceKafkaProduceHandler(
        transactional_id=build_transactional_id(
            stage=stage,
            consume_topic=consume_topic,
            instance_name=instance_name,
            worker_id=worker_id,
        )
    )


def _pipeline_mode(mode: str | None) -> str:
    selected_mode = (mode or KAFKA_SETTINGS.pipeline_mode).strip().lower()
    if selected_mode in {"exactly_once", "simple"}:
        return selected_mode
    raise ValueError(
        "KAFKA_PIPELINE_MODE must be 'exactly_once' or 'simple', "
        f"got {selected_mode!r}."
    )

"""Value objects passed between Kafka consumers and producers."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ConsumedKafkaMessage:
    """Decoded Kafka record plus the source offset required for EoS."""

    key: str | None
    value: str | None
    topic: str
    partition: int
    offset: int
    raw_message: object | None = field(default=None, repr=False, compare=False)


@dataclass(frozen=True)
class KafkaProduceRecord:
    """One output record queued for a Kafka transaction."""

    topic: str
    data: str
    key: str | None = None

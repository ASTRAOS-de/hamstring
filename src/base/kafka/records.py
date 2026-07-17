"""Value objects exchanged by Kafka consumers and producers."""

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class ConsumedKafkaMessage:
    """A consumed record and the source offset needed for acknowledgement."""

    key: str | None
    value: str | None
    topic: str
    partition: int
    offset: int
    headers: tuple[tuple[str, bytes | None], ...] = ()


@dataclass(frozen=True)
class KafkaProduceRecord:
    """A record to publish through either producer implementation."""

    topic: str
    data: str
    key: str | None = None
    headers: tuple[tuple[str, bytes | None], ...] = ()

    def produce_kwargs(self, callback: Callable) -> dict[str, Any]:
        """Return the keyword arguments expected by confluent-kafka."""
        kwargs = {
            "topic": self.topic,
            "key": self.key,
            "value": self.data,
            "callback": callback,
        }
        if self.headers:
            kwargs["headers"] = list(self.headers)
        return kwargs

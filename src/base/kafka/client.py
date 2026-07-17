"""Common base type for Kafka client wrappers."""


class KafkaHandler:
    """Base class shared by producer and consumer wrappers."""

    def __init__(self) -> None:
        self.consumer = None

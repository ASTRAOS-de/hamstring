"""Kafka-specific transient error classification."""

from confluent_kafka import KafkaError, KafkaException


def is_retriable_kafka_exception(exception: Exception) -> bool:
    return isinstance(exception, (KafkaException, BufferError, RuntimeError, OSError))


def is_retriable_kafka_error(error) -> bool:
    retriable = getattr(error, "retriable", None)
    if callable(retriable) and retriable():
        return True

    retriable_codes = {
        getattr(KafkaError, name)
        for name in (
            "_ALL_BROKERS_DOWN",
            "_TRANSPORT",
            "_TIMED_OUT",
            "_MSG_TIMED_OUT",
            "_RESOLVE",
            "_WAIT_COORD",
        )
        if hasattr(KafkaError, name)
    }
    return hasattr(error, "code") and error.code() in retriable_codes

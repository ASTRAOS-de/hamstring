"""Kafka-specific transient error classification."""

from confluent_kafka import KafkaError, KafkaException


_CONSUMER_MEMBERSHIP_ERROR_CODES = {
    getattr(KafkaError, name)
    for name in (
        "UNKNOWN_MEMBER_ID",
        "ILLEGAL_GENERATION",
        "REBALANCE_IN_PROGRESS",
        "FENCED_INSTANCE_ID",
        "_MAX_POLL_EXCEEDED",
    )
    if hasattr(KafkaError, name)
}


def kafka_error_from_exception(exception: Exception):
    """Return the Kafka error wrapped by ``KafkaException``, when present."""
    if not isinstance(exception, KafkaException) or not exception.args:
        return None
    error = exception.args[0]
    return error if callable(getattr(error, "code", None)) else None


def is_consumer_membership_error(error) -> bool:
    """Whether an error requires abandoning work tied to the old assignment."""
    code = getattr(error, "code", None)
    return callable(code) and code() in _CONSUMER_MEMBERSHIP_ERROR_CODES


def is_consumer_membership_exception(exception: Exception) -> bool:
    """Whether a Kafka exception wraps a consumer membership error."""
    return is_consumer_membership_error(kafka_error_from_exception(exception))


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

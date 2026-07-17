"""Kafka-specific transient error classification."""

from confluent_kafka import KafkaError, KafkaException


def is_retriable_kafka_exception(exception: BaseException) -> bool:
    if isinstance(exception, (BufferError, OSError)):
        return True
    if isinstance(exception, KafkaException):
        error = exception.args[0] if exception.args else None
        return error is None or is_retriable_kafka_error(error)
    return False


def is_retriable_kafka_error(error: KafkaError) -> bool:
    if error.retriable():
        return True

    return error.code() in {
        KafkaError._ALL_BROKERS_DOWN,
        KafkaError._TRANSPORT,
        KafkaError._TIMED_OUT,
        KafkaError._MSG_TIMED_OUT,
        KafkaError._RESOLVE,
        KafkaError._WAIT_COORD,
        KafkaError._UNKNOWN_TOPIC,
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError.UNKNOWN_TOPIC_ID,
    }

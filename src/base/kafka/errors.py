"""Kafka wrapper exceptions."""


class TooManyFailedAttemptsError(Exception):
    """Raised when required Kafka topics never become available."""


class KafkaMessageFetchException(Exception):
    """Raised when Kafka returns a permanent consumer error."""


class KafkaConsumerMembershipLost(Exception):
    """Raised when a transaction uses stale consumer-group membership."""

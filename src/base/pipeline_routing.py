"""Pipeline-specific Kafka routing conventions."""

import ipaddress
import json

SERVER_MESSAGE_ID_HEADER = "hamstring-server-message-id"


def source_ip_routing_key(message: str) -> str | None:
    """Return a source-IP key, or no key when input validation must reject it."""
    try:
        source_ip = json.loads(message)["src_ip"]
        return str(ipaddress.ip_address(source_ip))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def server_message_id_from_headers(
    headers: tuple[tuple[str, bytes | None], ...],
) -> str | None:
    """Decode the LogServer trace ID carried on the first pipeline hop."""
    value = dict(headers).get(SERVER_MESSAGE_ID_HEADER)
    if value is None:
        return None
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)

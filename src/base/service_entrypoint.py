"""Container entrypoint that waits for service dependencies before startup."""

from __future__ import annotations

import http.client
import logging
import os
import sys
import time
from collections.abc import Callable

import yaml
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient


logging.basicConfig(
    format="[%(asctime)s, %(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
CONFIG_FILEPATH = os.path.join(os.path.dirname(__file__), "../../config.yaml")
DEFAULT_CLICKHOUSE_HTTP_PORT = 8123


def parse_dependency_names(raw_dependencies: str | None) -> list[str]:
    if not raw_dependencies:
        return []

    return [
        dependency.strip().lower()
        for dependency in raw_dependencies.split(",")
        if dependency.strip()
    ]


def parse_host_port_list(raw_endpoints: str | None) -> list[tuple[str, int]]:
    if not raw_endpoints:
        return []

    endpoints: list[tuple[str, int]] = []
    for raw_endpoint in raw_endpoints.split(","):
        endpoint = raw_endpoint.strip()
        if not endpoint:
            continue

        host, separator, raw_port = endpoint.rpartition(":")
        if not separator or not host or not raw_port:
            raise ValueError(f"Invalid endpoint {endpoint!r}; expected host:port")

        endpoints.append((host, int(raw_port)))

    return endpoints


def get_kafka_endpoints(config: dict) -> list[tuple[str, int]]:
    env_endpoints = parse_host_port_list(os.getenv("HAMSTRING_KAFKA_WAIT_ENDPOINTS"))
    if env_endpoints:
        return env_endpoints

    endpoints = []
    for broker in config["environment"]["kafka_brokers"]:
        endpoints.append((broker["hostname"], int(broker["internal_port"])))
    return endpoints


def get_clickhouse_endpoint(config: dict) -> tuple[str, int]:
    env_endpoints = parse_host_port_list(
        os.getenv("HAMSTRING_CLICKHOUSE_WAIT_ENDPOINT")
    )
    if env_endpoints:
        return env_endpoints[0]

    clickhouse_config = config["environment"]["monitoring"]["clickhouse_server"]
    return (
        clickhouse_config["hostname"],
        int(clickhouse_config.get("http_port", DEFAULT_CLICKHOUSE_HTTP_PORT)),
    )


def setup_config() -> dict:
    with open(CONFIG_FILEPATH, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def can_query_kafka(admin_client: AdminClient, timeout_seconds: float) -> bool:
    try:
        admin_client.list_topics(timeout=timeout_seconds)
        return True
    except KafkaException:
        return False


def can_ping_clickhouse(host: str, port: int, timeout_seconds: float) -> bool:
    connection = http.client.HTTPConnection(host, port, timeout=timeout_seconds)
    try:
        connection.request("GET", "/ping")
        response = connection.getresponse()
        response.read()
        return response.status == 200
    except OSError:
        return False
    finally:
        connection.close()


def wait_until_ready(
    name: str,
    check: Callable[[], bool],
    timeout_seconds: int,
    interval_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds

    while True:
        if check():
            logger.info("%s is reachable.", name)
            return

        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting for {name}.")

        logger.info("Waiting for %s...", name)
        time.sleep(interval_seconds)


def wait_for_dependencies(dependencies: list[str]) -> None:
    if not dependencies:
        return

    config = setup_config()
    timeout_seconds = int(os.getenv("HAMSTRING_WAIT_TIMEOUT_SECONDS", "180"))
    interval_seconds = float(os.getenv("HAMSTRING_WAIT_INTERVAL_SECONDS", "2"))
    initial_delay_seconds = float(
        os.getenv("HAMSTRING_WAIT_INITIAL_DELAY_SECONDS", "30")
    )

    if initial_delay_seconds > 0:
        logger.info(
            "Waiting %.1fs before dependency checks.",
            initial_delay_seconds,
        )
        time.sleep(initial_delay_seconds)

    for dependency in dependencies:
        if dependency == "kafka":
            endpoints = get_kafka_endpoints(config)
            bootstrap_servers = ",".join(
                f"{host}:{port}" for host, port in endpoints
            )
            admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
            wait_until_ready(
                f"kafka at {bootstrap_servers}",
                lambda: can_query_kafka(admin_client, timeout_seconds=2),
                timeout_seconds,
                interval_seconds,
            )
        elif dependency == "clickhouse":
            host, port = get_clickhouse_endpoint(config)
            wait_until_ready(
                f"clickhouse at {host}:{port}",
                lambda: can_ping_clickhouse(host, port, timeout_seconds=2),
                timeout_seconds,
                interval_seconds,
            )
        else:
            raise ValueError(
                f"Unsupported dependency {dependency!r}. "
                "Supported dependencies: kafka, clickhouse."
            )


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit(
            "Usage: python -m src.base.service_entrypoint -m <module> [args...]"
        )

    wait_for_dependencies(parse_dependency_names(os.getenv("HAMSTRING_WAIT_FOR")))
    os.execv(sys.executable, [sys.executable, *sys.argv[1:]])


if __name__ == "__main__":
    main()

import os

import yaml

from src.base.log_config import get_logger

logger = get_logger()

CONFIG_FILEPATH = os.path.join(os.path.dirname(__file__), "../../config.yaml")


def get_zeek_sensor_topic_base_names(config: dict) -> set:
    """
    Method to retrieve the protocols monitored by the zeek sensors based on the ``config.yaml``

    Args:
        config (dict): The configuration dictionary from config.yaml

    Returns:
        Set of protocol names the zeek sensors are monitoring, e.g. (dns, http, sftp, ... )
    """
    return {
        protocol
        for sensor in config["pipeline"]["zeek"]["sensors"].values()
        for protocol in sensor.get("protocols", [])
    }


# TODO: test this method!
def get_batch_configuration(collector_name: str) -> dict:
    """
    Method to combine custom batch_handler configuartions per logcollector with the default ones.
    Yields a dict where custom configurations override default ones. If no custom value is specified,
    deafult values are returned.

    Args:
        collector_name (str): Name of the collector to retrieve the configuration for
    Returns:
        Dictionairy with the complete batch_handler configuration (e.g. ipv4_prefix_length, batch_size, etc. )
    """
    config = setup_config()
    default_configuration = config["pipeline"]["log_collection"][
        "default_batch_handler_config"
    ]
    collector_configs = config["pipeline"]["log_collection"]["collectors"]

    for collector in collector_configs:
        if collector["name"] == collector_name:
            override = collector.get("batch_handler_config_override")
            if override:
                # Merge override into a copy of the default configuration
                merged = {**default_configuration, **override}
                return merged

    return default_configuration


def setup_config():
    """Load and return the application configuration from the YAML configuration file.

    Reads the configuration file from the predefined CONFIG_FILEPATH and parses
    it as a YAML document. This function provides centralized configuration
    loading for the entire application.

    Returns:
        dict: Configuration data as a Python dictionary containing all
              application settings and parameters.

    Raises:
        FileNotFoundError: If the configuration file does not exist at the
                           expected path.
        yaml.YAMLError: If the configuration file contains invalid YAML syntax.
    """
    try:
        logger.debug(f"Opening configuration file at {CONFIG_FILEPATH}...")
        with open(CONFIG_FILEPATH, "r") as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        logger.critical(f"File {CONFIG_FILEPATH} does not exist. Aborting...")
        raise

    logger.debug("Configuration file successfully opened and information returned.")
    return config

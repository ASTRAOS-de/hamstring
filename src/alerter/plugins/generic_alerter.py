
from src.alerter.alerter import AlerterBase
from src.base.log_config import get_logger

module_name = "src.alerter.generic_alerter"
logger = get_logger(module_name)


class GenericAlerter(AlerterBase):
    """
    Specific implementation for an Alerter that processes alerts
    from a generic topic.

    It performs no additional processing or transformation by itself,
    instead relying solely on the base actions (logging to file/Kafka).
    """

    def process_alert(self):
        """
        Generic implementation: no special processing needed.
        """
        pass

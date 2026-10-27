from src.detector.detector import DetectorBase
import numpy as np
from collections import defaultdict
from datetime import datetime, timedelta

from src.base.log_config import get_logger
from src.detector.plugins.domainator_utils import (
    strip_domain,
    get_domainator_features,
)

module_name = "data_analysis.detector"
logger = get_logger(module_name)


class DomainatorDetector(DetectorBase):
    """
    Detector implementation for identifying data exfiltration and command and control on the
    subdomain level.

    This class extends the DetectorBase to provide specific functionality for detecting
    malicious queries. It analyzes subdomain similarity characteristics based on grouping
    of the queries in windows of fixed size, in order to identify potential data exfiltration
    or command and control.

    The detector extracts various statistical similarity features from windows of subdomains
    to make predictions about whether a query is likely malicious.
    """

    def __init__(
        self,
        detector_config,
        consume_topic,
        produce_topics=None,
        downstream_detector_topics=None,
    ):
        """
        Initialize the Domainator detector with configuration parameters.

        Sets up the detector with the model base URL and passes configuration to the
        base class for standard detector initialization.

        Args:
            detector_config (dict): Configuration dictionary containing detector-specific
                parameters including base_url, model, checksum, and threshold.
            consume_topic (str): Kafka topic from which the detector will consume messages.
        """
        self.model_base_url = detector_config["base_url"]
        # How long a domain has to have been stale for before it gets removed from the dictionary
        self.domain_expiration_hours = timedelta(hours=detector_config["domain_expiration_hours"])
        # How often the detector checks whether entries have expired
        self.expiry_check_interval = timedelta(hours=detector_config['expiry_check_interval_hours'])

        self.message_queues = defaultdict(
            lambda: defaultdict(lambda: {"timestamp": None, "messages": []})
        )
        self.latest_message_timestamp = None
        self.latest_expiry_timestamp = None

        self.requests_detect_threshold = 10
        self.max_window_requests = 10
        self.max_window_cut = 5

        super().__init__(
            detector_config, consume_topic, produce_topics, downstream_detector_topics
        )

    def expire_stale_domains(self):
        """
        Drop windows for src_ip/domain pairs that have not been seen for a certain period of time. 

        The method deliberately avoids using the current time and instead tracks the
        timestamp of the observed data. This keeps the tracking correct for both live
        traffic and historical data.

        Returns:
            int: Number of ``(src_ip, domain)`` entries removed.
        """
        removed_domains = 0

        stale_entries = []
        for src, domains in self.message_queues.items():
            for domain, entry in domains.items():
                entry_timestamp = datetime.fromisoformat(entry["timestamp"])
                if self.latest_message_timestamp - entry_timestamp > self.domain_expiration_hours:
                    stale_entries.append((src, domain))

        for src, domain in stale_entries:
            del self.message_queues[src][domain]
            removed_domains += 1

            if not self.message_queues[src]:
                del self.message_queues[src]

        return removed_domains



    def predict(self, messages):
        """
        Process a window of messages and predict if the domain is likely to be used
        for malicious exfiltration and communication.

        Extracts features from the subdomains in the messages and uses the loaded
        machine learning model to generate prediction probabilities.

        Args:
            message (list): A list containing the messages data, expected to have
                a "domain_name" key with the domain to analyze.

        Returns:
            np.ndarray: Prediction probabilities for each class. Typically a 2D array
                where the shape is (1, 2) for binary classification (benign/malicious).
        """
        queries = [message["domain_name"] for message in messages]

        y_pred = self.model.predict_proba(get_domainator_features(queries))
        y_pred = dict(zip(self.model.classes_, y_pred[0]))

        return y_pred

    def detect(self):
        logger.info("Start detecting malicious requests.")
        for message in self.messages:
            message_src = message["src_ip"]
            message_domain = strip_domain(message["domain_name"])
            message_queue = self.message_queues[message_src][message_domain]

            message_queue["messages"].append(message)
            message_queue["timestamp"] = message["ts"]

            datetime_timestamp = datetime.fromisoformat(message_queue["timestamp"])
            if self.latest_message_timestamp is None or datetime_timestamp > self.latest_message_timestamp:
                self.latest_message_timestamp = datetime_timestamp

            # currently receives 1 message batch at a time iterates over it and adds to the queue
            # right now let's say we fetch 100 messages in a batch, we iterate over the messages to add to the dict, worst case: 97 predictions necessary (len of 3, sliding window over all 100 messsages)
            # adds up over time as no reset of the dict happens --> at worst 100 diffrerent domains in a batch --> 100 predicitons + 100 predictions in downstream detectors!
            # ---> too much
            # improvement suggestion: try to predict only at the end for each domain seen in the batch IF len > 3. If prediction happened maybe clear the queue ? --> suboptimal for no sliding window approach
            # further improvement: kafka partitoin key shold not be src_ip but rather domain name --> make adjustable for other detectors as well !!

            if len(message_queue["messages"]) >= self.requests_detect_threshold:
                y_pred = self.predict(message_queue["messages"])
                logger.info(f"Prediction: {y_pred}")
                if y_pred["malicious"] == max(y_pred.values()) and y_pred["malicious"] > self.threshold:
                    logger.info("Append malicious request domain to warning.")
                    warning = {
                        "request": message_queue["messages"].copy(),
                        "probability": float(y_pred["malicious"]),
                        "name": self.name,
                        "sha256": self.checksum,
                    }
                    self.warnings.append(warning)

                # The statement here can be removed if the minimum and maximum window sizes are kept the same
                if len(message_queue["messages"]) >= self.max_window_requests:
                    del message_queue["messages"][0:self.max_window_cut]

        # The check is done based on the message's timestamps to support historical data
        if self.latest_expiry_timestamp is None or self.latest_message_timestamp - self.latest_expiry_timestamp > self.expiry_check_interval:
            print(self.latest_expiry_timestamp, self.latest_message_timestamp)
            removed_domains = self.expire_stale_domains()
            self.latest_expiry_timestamp = self.latest_message_timestamp
            if removed_domains:
                logger.info(f"Expired {removed_domains} stale window(s).")

        logger.debug(f"Domainator Message queue length: {len(self.message_queues)}")

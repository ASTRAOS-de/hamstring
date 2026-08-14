from src.detector.detector import DetectorBase
import numpy as np
from collections import defaultdict
import itertools
import pylcs
import Levenshtein

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
        self.message_queues = defaultdict(list)
        super().__init__(
            detector_config, consume_topic, produce_topics, downstream_detector_topics
        )

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
        return y_pred

    def detect(self):
        logger.info("Start detecting malicious requests.")
        for message in self.messages:
            message_domain = strip_domain(message["domain_name"])
            self.message_queues[message_domain].append(message)

            # currently receives 1 message batch at a time iterates over it and adds to the queue
            # right now let's say we fetch 100 messages in a batch, we iterate over the messages to add to the dict, worst case: 97 predictions necessary (len of 3, sliding window over all 100 messsages)
            # adds up over time as no reset of the dict happens --> at worst 100 diffrerent domains in a batch --> 100 predicitons + 100 predictions in downstream detectors!
            # ---> too much
            # improvement suggestion: try to predict only at the end for each domain seen in the batch IF len > 3. If prediction happened maybe clear the queue ? --> suboptimal for no sliding window approach
            # further improvement: kafka partitoin key shold not be src_ip but rather domain name --> make adjustable for other detectors as well !!

            if len(self.message_queues[message_domain]) >= 3:
                y_pred = self.predict(self.message_queues[message_domain])
                logger.info(f"Prediction: {y_pred}")
                if np.argmax(y_pred, axis=1) == 1 and y_pred[0][1] > self.threshold:
                    logger.info("Append malicious request domain to warning.")
                    warning = {
                        "request": self.message_queues[message_domain],
                        "probability": float(y_pred[0][1]),
                        "name": self.name,
                        "sha256": self.checksum,
                    }
                    self.warnings.append(warning)

                if len(self.message_queues[message_domain]) >= 10:
                    del self.message_queues[message_domain][0]
        logger.debug(f"Domainator Message queue length: {len(self.message_queues)}")

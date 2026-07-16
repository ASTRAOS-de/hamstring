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

LEGITIMATE_ATTRIBUTE_LABELS = {"benign", "legit", "legitimate", "legitimate_legitimate"}


class DomainatorAttributor(DetectorBase):
    """
    Detector implementation for the attribution of a tool or malware that was used in the
    data exfiltration and command and control, using the subdomain level labels.

    This class extends the DetectorBase to provide specific functionality for identifying
    the tool sending the malicious queries. It analyzes subdomain similarity characteristics
    based on grouping of the queries in windows of fixed size, similar to the Domainator detector
    approach. It can be used both as a standalone detector or as a next stage in a pipeline of detectors,
    dependent on the provided model.

    The identity detector extracts various statistical similarity features from windows of subdomains
    to make predictions about what tool likely sent the malicious query or what job/behaviour was observed.
    """

    def __init__(
        self,
        detector_config,
        consume_topic,
        produce_topics=None,
        downstream_detector_topics=None,
        worker_id="default",
    ):
        """
        Initialize the Domainator attributor with configuration parameters.

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
            detector_config,
            consume_topic,
            produce_topics,
            downstream_detector_topics,
            worker_id,
        )

        self.labels = self.model.classes_

    def predict(self, messages):
        """
        Process a window of messages and predict what tool was likely used
        to sent a potentially malicious exfiltration and communication.

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
            if isinstance(message, list):
                # This assumes the current example for the request structure.
                # Both DomainatorDetector and DomainatorAttributor provide a list to the 'request' key,
                # due to their structure of processing a window (list) of incoming messages.
                # Would be better to have a key:value pair in request that defines the domain name outside of the list?
                message_domain = strip_domain(message[0]["domain_name"])
                self.message_queues[message_domain].extend(message)
            else:
                message_domain = strip_domain(message["domain_name"])
                self.message_queues[message_domain].append(message)

            if len(self.message_queues[message_domain]) >= 3:
                y_pred = self.predict(self.message_queues[message_domain])
                logger.info(f"Prediction: {y_pred}")

                winning_index = int(np.argmax(y_pred, axis=1)[0])
                winning_label = self.labels[winning_index]
                winning_probability = float(y_pred[0][winning_index])
                y_pred_labelled = [
                    {"attribute": label, "probability": float(score)}
                    for label, score in zip(self.labels, y_pred[0])
                    if score >= self.threshold
                ]
                logger.debug(f"Prediction with labels: {y_pred_labelled}")

                is_legitimate = winning_label in LEGITIMATE_ATTRIBUTE_LABELS
                if not is_legitimate and winning_probability >= self.threshold:
                    logger.debug("Append malicious request domain to warning.")
                    warning = {
                        "request": self.message_queues[message_domain],
                        "probability": winning_probability,
                        "predicted_class": winning_label,
                        "attributes": y_pred_labelled,
                        "name": self.name,
                        "sha256": self.checksum,
                    }
                    self.warnings.append(warning)

                if len(self.message_queues[message_domain]) >= 10:
                    del self.message_queues[message_domain][0]

import math
import numpy as np
import unittest
from unittest.mock import MagicMock, patch, call
from pandas.testing import assert_frame_equal

import os
import sys

sys.path.append(os.getcwd())

from src.detector.plugins.domainator_detector import DomainatorDetector
from src.base.data_classes.batch import Batch
from src.detector.plugins.domainator_utils import (
    DOMAINATOR_FEATURE_COLUMNS,
    get_domainator_features
)


DEFAULT_DATA = {
    "src_ip": "192.168.0.167",
    "dns_ip": "10.10.0.10",
    "response_ip": "252.79.173.222",
    "ts": "",
    "status": "NXDOMAIN",
    "domain_name": "IF356gEnJHPdRxnkDId4RDUSgtqxx9I+pZ5n1V53MdghOGQncZWAQgAPRx3kswi.750jnH6iSqmiAAeyDUMX0W6SHGpVsVsKSX8ZkKYDs0GFh/9qU5N9cwl00XSD8ID.NNhBdHZIb7nc0hDQXFPlABDLbRwkJS38LZ8RMX4yUmR2Mb6YqTTJBn+nUcB9P+v.jBQdwdS53XV9W2p1BHjh.16.f.1.6037.tunnel.example.org",
    "record_type": "A",
    "size": "100b",
}


class TestDomainatorDetector(unittest.TestCase):
    def setUp(self):
        patcher = patch("src.detector.plugins.domainator_detector.logger")
        self.mock_logger = patcher.start()
        self.addCleanup(patcher.stop)

    def _create_detector(self, mock_kafka_handler=None, mock_clickhouse=None):
        """Helper method to create a DomainatorDetector instance with proper mocks."""
        if mock_kafka_handler is None:
            mock_kafka_handler = MagicMock()
        if mock_clickhouse is None:
            mock_clickhouse = MagicMock()

        detector_config = {
            "name": "domainator_detector",
            "detector_module_name": "domainator_detector",
            "detector_class_name": "DomainatorDetector",
            "model": "rf",
            "checksum": "9d86d66b4976c9b325bed0934a9a9eb3a20960b08be9afe491454624cc0aaa6c",
            "base_url": "https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/",
            "threshold": 0.005,
        }

        with patch(
            "src.detector.detector.ExactlyOnceKafkaConsumeHandler",
            return_value=mock_kafka_handler,
        ), patch(
            "src.detector.detector.ClickHouseKafkaSender", return_value=mock_clickhouse
        ), patch.object(
            DomainatorDetector, "_get_model", return_value=(MagicMock(), MagicMock())
        ):

            detector = DomainatorDetector(
                detector_config, "test_topic", ["test_produce_topic"]
            )
            detector.model = MagicMock()
            detector.scaler = MagicMock()
            return detector

    def test_get_model_download_url(self):
        """Test that the model download URL is correctly formatted."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)
        # overwrite model here again to not interefere with other tests when using it globally
        detector.model = "rf"
        self.maxDiff = None
        expected_url = "https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/files/?p=%2Frf%2F9d86d66b4976c9b325bed0934a9a9eb3a20960b08be9afe491454624cc0aaa6c%2Frf.pickle&dl=1"
        self.assertEqual(detector.get_model_download_url(), expected_url)

    def test_detect(self):
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        sut = self._create_detector(mock_kafka, mock_ch)
        for _ in range(0, 4, 1):
            sut.messages.append((DEFAULT_DATA))
        with patch(
            "src.detector.plugins.domainator_detector.DomainatorDetector.predict",
            return_value=[[0.01, 0.99]],
        ):
            sut.detect()
            self.assertNotEqual([], sut.warnings)

    def test_predict_calls_model(self):
        """Test that predict method correctly uses the model with features."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)

        # Mock model prediction
        mock_prediction = np.array([[0.2, 0.8]])
        detector.model.predict_proba.return_value = mock_prediction

        # Test prediction
        message = [{"domain_name": "google.com"}, {"domain_name": "google.com"}]
        result = detector.predict(message)

        # Verify model was called once
        detector.model.predict_proba.assert_called_once()

        # Verify the argument was correct
        called_features = detector.model.predict_proba.call_args[0][0]
        expected_features = get_domainator_features(["google.com", "google.com"])
        assert_frame_equal(called_features, expected_features)

        # Verify prediction result
        np.testing.assert_array_equal(result, mock_prediction)

    def test_predict_uses_domainator_feature_order(self):
        """Test that predict passes features in Domainator's generated order."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)

        mock_prediction = np.array([[0.2, 0.8]])
        detector.model.predict_proba.return_value = mock_prediction
        detector.model.feature_names_in_ = np.array(
            list(reversed(DOMAINATOR_FEATURE_COLUMNS))
        )

        message = [
            {"domain_name": "a.example.com"},
            {"domain_name": "b.example.com"},
            {"domain_name": "c.example.com"},
        ]
        result = detector.predict(message)

        called_features = detector.model.predict_proba.call_args[0][0]
        self.assertEqual(
            called_features.columns.tolist(),
            DOMAINATOR_FEATURE_COLUMNS,
        )
        np.testing.assert_array_equal(result, mock_prediction)

    def test_get_features_basic_attributes(self):
        """Test basic label features calculation."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)

        # Test with various 'google.com' subdomains
        features = get_domainator_features(
            ["sub1.google.com", "sub2.google.com", "sub3.google.com"]
        )

        # Basic features: label_length, label_max, label_average
        leven_dist = features.iloc[0, 0]  # Levenshtein distance
        jaro_dist = features.iloc[0, 1]  # Jaro distance
        lcs = features.iloc[0, 6]  # Longest common string

        self.assertEqual(leven_dist, 0.75)
        self.assertAlmostEqual(jaro_dist, 0.833, 3)  # Rounded to 3 decimal places
        self.assertEqual(lcs, 0.75)

    def test_get_features_empty_domains(self):
        """Test handling of empty domain strings."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)

        features = get_domainator_features(["", "", "", ""])

        # Basic features
        self.assertEqual(
            features.iloc[0, 0], 1.0
        )  # Levenshtein distance of empty strings is 1
        self.assertEqual(features.iloc[0, 1], 1.0)  # Jaro distance of empty strings is 1
        self.assertEqual(
            features.iloc[0, 2], 1.0
        )  # Jaro distance on the reverse empty strings is 1
        self.assertEqual(
            features.iloc[0, 3], 1.0
        )  # Jaro-Winkler distance of empty strings is 1
        self.assertEqual(
            features.iloc[0, 4], 1.0
        )  # Jaro-Winkler distance on the reverse empty strings is 1
        self.assertEqual(
            features.iloc[0, 5], 0.0
        )  # Longest common sequence of empty strings is 0
        self.assertEqual(
            features.iloc[0, 6], 0.0
        )  # Longest common string of empty strings is 0

    def test_get_features_single_same_character(self):
        """Test handling of single character domain."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)

        features = get_domainator_features(["a", "a", "a"])

        # Basic features
        self.assertEqual(
            features.iloc[0, 0], 1.0
        )  # Levenshtein distance of same strings is 1
        self.assertEqual(features.iloc[0, 1], 1.0)  # Jaro distance of same strings is 1
        self.assertEqual(
            features.iloc[0, 2], 1.0
        )  # Jaro distance on the reverse same strings is 1
        self.assertEqual(
            features.iloc[0, 3], 1.0
        )  # Jaro-Winkler distance of same strings is 1
        self.assertEqual(
            features.iloc[0, 4], 1.0
        )  # Jaro-Winkler distance on the reverse same strings is 1
        self.assertEqual(
            features.iloc[0, 5], 0.0
        )  # Longest common sequence of same strings is 0
        self.assertEqual(
            features.iloc[0, 6], 0.0
        )  # Longest common string of same strings is 0

    def test_get_features_feature_vector_shape(self):
        """Test that the feature vector has the expected shape."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)

        features = get_domainator_features(
            ["test.domain.com", "test.domain.com", "test.domain.com"]
        )

        expected_entropy = 7

        self.assertEqual(features.shape, (1, expected_entropy))
        self.assertEqual(features.columns.tolist(), DOMAINATOR_FEATURE_COLUMNS)

    def test_get_features_case_insensitivity(self):
        """Test that the statistical comparison is case-insensitive."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)

        features_upper = get_domainator_features(
            ["DRIVE.GOOGLE.COM", "WORKSPACE.GOOGLE.COM"]
        )
        features_lower = get_domainator_features(
            ["drive.google.com", "workspace.google.com"]
        )

        # The comparison features should be identical regardless of case
        np.testing.assert_array_almost_equal(
            features_upper.to_numpy()[0],
            features_lower.to_numpy()[0],
            decimal=5,
        )

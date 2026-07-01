import json
import unittest
from unittest.mock import patch, MagicMock

from src.base.kafka_handler import (
    build_consumer_group_id,
    ensure_topics,
    KafkaConsumeHandler,
    KafkaMessageFetchException,
    _desired_topic_partitions,
    _topic_replication_factor,
    _topic_config,
)


def _metadata(partitions_by_topic: dict[str, int]):
    metadata = MagicMock()
    metadata.topics = {}
    for topic, partition_count in partitions_by_topic.items():
        topic_metadata = MagicMock()
        topic_metadata.partitions = {
            partition: MagicMock() for partition in range(partition_count)
        }
        metadata.topics[topic] = topic_metadata
    return metadata


class TestConsumerGroupId(unittest.TestCase):
    @patch("src.base.kafka_handler.CONSUMER_GROUP_ID", "test_group_id")
    def test_build_consumer_group_id_for_single_topic(self):
        self.assertEqual(
            "test_group_id.test_topic",
            build_consumer_group_id("test_topic"),
        )

    @patch("src.base.kafka_handler.CONSUMER_GROUP_ID", "test_group_id")
    def test_build_consumer_group_id_for_multiple_topics(self):
        self.assertEqual(
            "test_group_id.test_topic_1__test_topic_2",
            build_consumer_group_id(["test_topic_2", "test_topic_1"]),
        )


class TestTopicReconciliation(unittest.TestCase):
    @patch("src.base.kafka_handler.NewTopic")
    def test_missing_topic_is_created_with_target_partitions(self, mock_new_topic):
        admin_client = MagicMock()
        admin_client.list_topics.side_effect = [
            _metadata({}),
            _metadata({"test_topic": 4}),
        ]
        admin_client.create_topics.return_value = {"test_topic": MagicMock()}
        mock_new_topic.side_effect = lambda topic, partitions, replication_factor: (
            topic,
            partitions,
            replication_factor,
        )

        target_partitions_by_topic = ensure_topics(
            admin_client,
            ["test_topic"],
            target_partitions=4,
            replication_factor=2,
        )

        self.assertEqual({"test_topic": 4}, target_partitions_by_topic)
        admin_client.create_topics.assert_called_once_with([("test_topic", 4, 2)])
        admin_client.create_partitions.assert_not_called()

    @patch("src.base.kafka_handler.NewPartitions")
    def test_existing_topic_with_too_few_partitions_is_expanded(
        self, mock_new_partitions
    ):
        admin_client = MagicMock()
        admin_client.list_topics.side_effect = [
            _metadata({"test_topic": 2}),
            _metadata({"test_topic": 2}),
        ]
        admin_client.create_partitions.return_value = {"test_topic": MagicMock()}
        mock_new_partitions.side_effect = lambda topic, total_count: (
            topic,
            total_count,
        )

        ensure_topics(
            admin_client,
            ["test_topic"],
            target_partitions=4,
            replication_factor=2,
        )

        admin_client.create_topics.assert_not_called()
        admin_client.create_partitions.assert_called_once_with([("test_topic", 4)])

    def test_existing_topic_with_enough_partitions_is_left_unchanged(self):
        admin_client = MagicMock()
        admin_client.list_topics.side_effect = [
            _metadata({"test_topic": 8}),
            _metadata({"test_topic": 8}),
        ]

        ensure_topics(
            admin_client,
            ["test_topic"],
            target_partitions=4,
            replication_factor=2,
        )

        admin_client.create_topics.assert_not_called()
        admin_client.create_partitions.assert_not_called()

    @patch("src.base.retry.time.sleep", return_value=None)
    def test_metadata_lookup_retries_until_kafka_is_available(self, mock_sleep):
        admin_client = MagicMock()
        admin_client.list_topics.side_effect = [
            RuntimeError("broker unavailable"),
            _metadata({"test_topic": 4}),
            _metadata({"test_topic": 4}),
        ]

        target_partitions_by_topic = ensure_topics(
            admin_client,
            ["test_topic"],
            target_partitions=4,
            replication_factor=2,
        )

        self.assertEqual({"test_topic": 4}, target_partitions_by_topic)
        self.assertEqual(3, admin_client.list_topics.call_count)
        mock_sleep.assert_called()

    def test_auto_expand_can_be_disabled(self):
        admin_client = MagicMock()
        admin_client.list_topics.return_value = _metadata({"test_topic": 2})

        ensure_topics(
            admin_client,
            ["test_topic"],
            target_partitions=4,
            replication_factor=2,
            auto_expand_partitions=False,
        )

        admin_client.create_topics.assert_not_called()
        admin_client.create_partitions.assert_not_called()

    @patch("src.base.kafka_handler.NUMBER_OF_INSTANCES", 6)
    @patch("src.base.kafka_handler.KAFKA_TOPIC_DEFAULT_PARTITIONS", 3)
    def test_desired_partitions_uses_highest_scale_value(self):
        self.assertEqual(6, _desired_topic_partitions())

    @patch("src.base.kafka_handler.KAFKA_TOPIC_REPLICATION_FACTOR", 3)
    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [{"hostname": "127.0.0.1", "internal_port": 9999}],
    )
    def test_replication_factor_is_capped_to_configured_brokers(self):
        self.assertEqual(1, _topic_replication_factor())

    @patch(
        "src.base.kafka_handler.KAFKA_PIPELINE_TOPIC_PREFIXES",
        {"inspector_to_detector": "pipeline-inspector_to_detector"},
    )
    @patch(
        "src.base.kafka_handler.KAFKA_TOPIC_STAGE_CONFIG",
        {"inspector_to_detector": {"partitions": 7, "replication_factor": 2}},
    )
    def test_stage_topic_config_is_resolved_from_prefix(self):
        topic = "pipeline-inspector_to_detector-domainator"
        self.assertEqual(
            {"partitions": 7, "replication_factor": 2}, _topic_config(topic)
        )
        self.assertEqual(7, _desired_topic_partitions(topic))
        self.assertEqual(2, _topic_replication_factor(topic))

    @patch(
        "src.base.kafka_handler.KAFKA_TOPIC_EXACT_CONFIG",
        {"hamstring_alerts": {"partitions": 5, "replication_factor": 2}},
    )
    def test_exact_topic_config_wins(self):
        self.assertEqual(
            {"partitions": 5, "replication_factor": 2},
            _topic_config("hamstring_alerts"),
        )
        self.assertEqual(5, _desired_topic_partitions("hamstring_alerts"))
        self.assertEqual(2, _topic_replication_factor("hamstring_alerts"))


class TestInit(unittest.TestCase):
    @patch("src.base.kafka_handler.CONSUMER_GROUP_ID", "test_group_id")
    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "internal_port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "internal_port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "internal_port": 9997,
            },
        ],
    )
    @patch(
        "src.base.kafka_handler.KafkaConsumeHandler._all_topics_created",
        return_value=True,
    )
    @patch("src.base.kafka_handler.AdminClient")
    @patch("src.base.kafka_handler.Consumer")
    def test_init_successful(
        self, mock_consumer, mock_admin_client, mock_all_topics_created
    ):
        # Arrange
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance

        expected_conf = {
            "bootstrap.servers": "127.0.0.1:9999,127.0.0.2:9998,127.0.0.3:9997",
            "group.id": "test_group_id.test_topic",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
            "max.poll.interval.ms": 1800000,
        }

        # Act
        sut = KafkaConsumeHandler(topics="test_topic")

        # Assert
        self.assertEqual(mock_consumer_instance, sut.consumer)

        mock_consumer.assert_called_once_with(expected_conf)
        mock_consumer_instance.subscribe.assert_called_once()

    @patch("src.base.kafka_handler.CONSUMER_GROUP_ID", "test_group_id")
    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "internal_port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "internal_port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "internal_port": 9997,
            },
        ],
    )
    @patch(
        "src.base.kafka_handler.KafkaConsumeHandler._all_topics_created",
        side_effect=[False, True],
    )
    @patch("src.base.retry.time.sleep", return_value=None)
    @patch("src.base.kafka_handler.AdminClient")
    @patch("src.base.kafka_handler.Consumer")
    def test_init_retries_until_topics_are_visible(
        self, mock_consumer, mock_admin_client, mock_sleep, mock_all_topics_created
    ):
        # Arrange
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance

        expected_conf = {
            "bootstrap.servers": "127.0.0.1:9999,127.0.0.2:9998,127.0.0.3:9997",
            "group.id": "test_group_id.test_topic",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
            "max.poll.interval.ms": 1800000,
        }

        # Act
        sut = KafkaConsumeHandler(topics="test_topic")

        # Assert
        self.assertEqual(mock_consumer_instance, sut.consumer)
        self.assertEqual(2, mock_consumer.call_count)
        mock_consumer.assert_any_call(expected_conf)
        mock_consumer_instance.close.assert_called_once()
        mock_consumer_instance.subscribe.assert_called_once()
        mock_sleep.assert_called()

    @patch("src.base.kafka_handler.CONSUMER_GROUP_ID", "test_group_id")
    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "internal_port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "internal_port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "internal_port": 9997,
            },
        ],
    )
    @patch(
        "src.base.kafka_handler.KafkaConsumeHandler._all_topics_created",
        return_value=True,
    )
    @patch("src.base.kafka_handler.AdminClient")
    @patch("src.base.kafka_handler.Consumer")
    def test_init_successful_with_list(
        self, mock_consumer, mock_admin_client, mock_all_topics_created
    ):
        # Arrange
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance

        expected_conf = {
            "bootstrap.servers": "127.0.0.1:9999,127.0.0.2:9998,127.0.0.3:9997",
            "group.id": "test_group_id.test_topic_1__test_topic_2",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
            "max.poll.interval.ms": 1800000,
        }

        # Act
        sut = KafkaConsumeHandler(topics=["test_topic_1", "test_topic_2"])

        # Assert
        self.assertEqual(mock_consumer_instance, sut.consumer)

        mock_consumer.assert_called_once_with(expected_conf)
        mock_consumer_instance.subscribe.assert_called_once()


class TestConsume(unittest.TestCase):
    @patch("src.base.kafka_handler.CONSUMER_GROUP_ID", "test_group_id")
    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "internal_port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "internal_port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "internal_port": 9997,
            },
        ],
    )
    @patch(
        "src.base.kafka_handler.KafkaConsumeHandler._all_topics_created",
        return_value=True,
    )
    @patch("src.base.kafka_handler.AdminClient")
    @patch("src.base.kafka_handler.Consumer")
    def test_not_implemented(
        self, mock_consumer, mock_admin_client, mock_all_topics_created
    ):
        # Arrange
        sut = KafkaConsumeHandler(topics="test_topic")

        # Act and Assert
        with self.assertRaises(NotImplementedError):
            sut.consume()


class TestConsumeAsJSON(unittest.TestCase):
    @patch("src.base.kafka_handler.CONSUMER_GROUP_ID", "test_group_id")
    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "internal_port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "internal_port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "internal_port": 9997,
            },
        ],
    )
    @patch(
        "src.base.kafka_handler.KafkaConsumeHandler._all_topics_created",
        return_value=True,
    )
    @patch("src.base.kafka_handler.AdminClient")
    @patch("src.base.kafka_handler.Consumer")
    def setUp(self, mock_consumer, mock_admin_client, mock_all_topics_created):
        self.sut = KafkaConsumeHandler(topics="test_topic")

    def test_successful(self):
        with patch(
            "src.base.kafka_handler.KafkaConsumeHandler.consume"
        ) as mock_consume:
            # Arrange
            mock_consume.return_value = [
                "test_key",
                json.dumps(dict(test_value=123)),
                "test_topic",
            ]

            # Act
            returned_values = self.sut.consume_as_json()

        # Assert
        self.assertEqual(("test_key", dict(test_value=123)), returned_values)

    def test_wrong_data_format(self):
        with patch(
            "src.base.kafka_handler.KafkaConsumeHandler.consume"
        ) as mock_consume:
            # Arrange
            mock_consume.return_value = ["test_key", "wrong_format", "test_topic"]

            # Act and Assert
            with self.assertRaises(ValueError):
                self.sut.consume_as_json()

    def test_wrong_data_format_list(self):
        with patch(
            "src.base.kafka_handler.KafkaConsumeHandler.consume"
        ) as mock_consume:
            # Arrange
            mock_consume.return_value = [
                "test_key",
                json.dumps([1, 2, 3]),
                "test_topic",
            ]

            # Act and Assert
            with self.assertRaises(ValueError):
                self.sut.consume_as_json()

    def test_kafka_message_fetch_exception(self):
        with patch(
            "src.base.kafka_handler.KafkaConsumeHandler.consume",
            side_effect=KafkaMessageFetchException,
        ):
            # Act and Assert
            with self.assertRaises(KafkaMessageFetchException):
                self.sut.consume_as_json()

    def test_keyboard_interrupt(self):
        with patch(
            "src.base.kafka_handler.KafkaConsumeHandler.consume",
            side_effect=KeyboardInterrupt,
        ):
            # Act and Assert
            with self.assertRaises(KeyboardInterrupt):
                self.sut.consume_as_json()

    def test_kafka_message_else(self):
        with patch(
            "src.base.kafka_handler.KafkaConsumeHandler.consume"
        ) as mock_consume:
            # Arrange
            mock_consume.return_value = [None, None, "test_topic"]

            # Act
            returned_values = self.sut.consume_as_json()

        # Assert
        self.assertEqual((None, {}), returned_values)


class TestAllTopicsCreated(unittest.TestCase):
    @patch("src.base.kafka_handler.CONSUMER_GROUP_ID", "test_group_id")
    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "internal_port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "internal_port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "internal_port": 9997,
            },
        ],
    )
    @patch(
        "src.base.kafka_handler.KafkaConsumeHandler._all_topics_created",
        return_value=True,
    )
    @patch("src.base.kafka_handler.AdminClient")
    @patch("src.base.kafka_handler.Consumer")
    def setUp(self, mock_consumer, mock_admin_client, mock_all_topics_created):
        self.sut = KafkaConsumeHandler(topics=["test_topic", "another_topic"])

    @patch("src.base.kafka_handler.Consumer")
    def test_with_all_created(self, mock_consumer):
        # Arrange
        self.sut.consumer = MagicMock()
        self.sut.consumer.list_topics.return_value = _metadata(
            {"test_topic": 3, "another_topic": 3}
        )

        # Act and Assert
        self.assertTrue(
            self.sut._all_topics_created(
                topics=["test_topic", "another_topic"], min_partitions=3
            )
        )

    @patch("src.base.kafka_handler.time.sleep")
    @patch("src.base.kafka_handler.Consumer")
    def test_with_none_created(self, mock_consumer, mock_sleep):
        # Arrange
        mock_topics = MagicMock()
        mock_topics.topics = []

        self.sut.consumer = MagicMock()
        self.sut.consumer.list_topics.return_value = mock_topics

        # Act and Assert
        self.assertFalse(
            self.sut._all_topics_created(topics=["test_topic", "another_topic"])
        )

    @patch("src.base.kafka_handler.time.sleep")
    @patch("src.base.kafka_handler.Consumer")
    def test_with_too_few_partitions(self, mock_consumer, mock_sleep):
        # Arrange
        self.sut.consumer = MagicMock()
        self.sut.consumer.list_topics.return_value = _metadata(
            {"test_topic": 1, "another_topic": 3}
        )

        # Act and Assert
        self.assertFalse(
            self.sut._all_topics_created(
                topics=["test_topic", "another_topic"], min_partitions=3
            )
        )


if __name__ == "__main__":
    unittest.main()

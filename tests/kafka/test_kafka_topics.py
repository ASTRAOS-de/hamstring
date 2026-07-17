import unittest
from dataclasses import replace
from unittest.mock import Mock, patch

from src.base.kafka.config import KAFKA_SETTINGS
from src.base.kafka.topics import KafkaTopicManager, ensure_topics


class TestKafkaTopicManager(unittest.TestCase):
    def setUp(self):
        self.admin_client = Mock()
        self.topic_metadata = {"existing": self._topic_metadata(12)}
        self.admin_client.list_topics.side_effect = lambda timeout: Mock(
            topics=dict(self.topic_metadata)
        )

        def create_topics(topics):
            for topic in topics:
                self.topic_metadata[topic.topic] = self._topic_metadata(
                    topic.num_partitions
                )
            return {topic.topic: Mock() for topic in topics}

        self.admin_client.create_topics.side_effect = create_topics
        self.metadata_client_factory = Mock(return_value=self.admin_client)
        self.settings = replace(
            KAFKA_SETTINGS,
            topic_default_partitions=12,
            topic_replication_factor=2,
            topic_stage_config={"collector": {"partitions": 7}},
            topic_exact_config={"external": {"partitions": 3}},
            pipeline_topic_prefixes={"collector": "pipeline"},
        )

    @staticmethod
    def _topic_metadata(partition_count):
        return Mock(partitions={index: Mock() for index in range(partition_count)})

    def test_creates_each_missing_topic_from_its_own_configuration(self):
        KafkaTopicManager(
            self.admin_client,
            self.settings,
            self.metadata_client_factory,
        ).ensure(
            ["existing", "pipeline-dga", "external"]
        )

        new_topics = self.admin_client.create_topics.call_args.args[0]
        self.assertEqual(
            {
                ("pipeline-dga", 7, 2),
                ("external", 3, 2),
            },
            {
                (topic.topic, topic.num_partitions, topic.replication_factor)
                for topic in new_topics
            },
        )

    def test_existing_topics_are_not_resized_or_otherwise_modified(self):
        KafkaTopicManager(
            self.admin_client,
            self.settings,
            self.metadata_client_factory,
        ).ensure("existing")

        self.admin_client.create_topics.assert_not_called()

    @patch("src.base.retry.time.sleep")
    def test_waits_until_created_topics_are_visible(self, sleep):
        missing = Mock(topics={"existing": self._topic_metadata(12)})
        visible = Mock(
            topics={
                "existing": self._topic_metadata(12),
                "pipeline-dga": self._topic_metadata(7),
            }
        )
        self.admin_client.list_topics.side_effect = [missing, missing, visible]
        self.admin_client.create_topics.side_effect = lambda topics: {
            topic.topic: Mock() for topic in topics
        }

        KafkaTopicManager(
            self.admin_client,
            self.settings,
            self.metadata_client_factory,
        ).ensure("pipeline-dga")

        self.assertEqual(3, self.admin_client.list_topics.call_count)
        self.assertEqual(2, self.metadata_client_factory.call_count)
        sleep.assert_called_once()

    @patch("src.base.kafka.topics.KafkaTopicManager")
    @patch("src.base.kafka.topics.AdminClient")
    def test_public_helper_hides_admin_client_creation(
        self, admin_client_type, topic_manager_type
    ):
        ensure_topics("external", settings=self.settings)

        admin_client_type.assert_called_once_with(
            {"bootstrap.servers": self.settings.bootstrap_servers}
        )
        topic_manager_type.assert_called_once_with(
            admin_client_type.return_value, self.settings
        )
        topic_manager_type.return_value.ensure.assert_called_once_with(
            "external", target_partitions=None, replication_factor=None
        )


if __name__ == "__main__":
    unittest.main()

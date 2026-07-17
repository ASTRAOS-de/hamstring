import unittest

from src.base.kafka import KafkaHandler


class TestInit(unittest.TestCase):
    def test_init(self):
        sut = KafkaHandler()

        self.assertIsNone(sut.consumer)


if __name__ == "__main__":
    unittest.main()

"""
Test Writer Message Broker Kafka format wise
"""
import unittest
from unittest.mock import (
    MagicMock, patch,
)

from src.main.processor.writer.message_broker.kafka.writer_message_broker_kafka import (
    writer_message_broker_kafka_params,
)


class TestWriterMessageBrokerKafka(unittest.TestCase):
    """
    Unit tests for the Writer Message Broker Kafka format.
    """

    def setUp(self):
        """
        test setup method to initialize any required variables or configurations
        """
        self.mock_secret_instance_return_value = {
            "aws_sm_key_store_password": "test-key-store-password",
            "aws_sm_trust_store_password": "test-trust-store-password",
            "aws_sm_ssl_key_password": "test-ssl-key-password"
        }
        self.mock_config = {
            "aws_region": "test_region",
            "environment": "test_env",
            "logger_name": "test_logger",
            "processor_typ": "test_processor",
            "writer_typ": "test_message_broker_kafka",
            "outputMode": "test_mode",
            "secret_name": "test_secret_name",
            "stream_options": {
                "checkpointLocation": "test-location/checkpoint/",
            },
            "writer_typ_options": {
                "kafka.bootstrap.servers": "test-server",
                "kafka.security.protocol": "test-SSL",
                "topic": "test-topic",
                "kafka.ssl.keystore.location": "/path/test-key.jks",
                "kafka.ssl.keystore.password": "aws_sm_key_store_password",
                "kafka.ssl.truststore.location": "/path/test-trust.jks",
                "kafka.ssl.truststore.password": "aws_sm_trust_store_password",
                "kafka.ssl.key.password": "aws_sm_ssl_key_password",
                "startingOffsets": "test-latest"
            }
        }
        self.mock_secret_instance = MagicMock()

    @patch("src.main.processor.writer.message_broker.kafka.writer_message_broker_kafka.SecretUtils")
    def test_writer_message_broker_kafka_params(self, mock_secret_utils):
        """
        Test the writer_message_broker_kafka_params function to ensure it returns the correct parameters.
        :param: mock_secret_utils
        """
        mock_secret_utils.return_value = self.mock_secret_instance
        self.mock_secret_instance.get_secret.return_value = self.mock_secret_instance_return_value

        actual_writer_config: dict = writer_message_broker_kafka_params(
            config=self.mock_config,
        )
        mock_secret_utils.assert_called_once_with(
            region_name=self.mock_config["aws_region"],
            logger=self.mock_config["logger_name"],
        )
        mock_secret_utils.return_value.get_secret.assert_called_once_with(
            secret_name=self.mock_config["secret_name"],
        )
        self.assertEqual(
            actual_writer_config["writer_typ"],
            "message_broker_kafka"
        )
        self.assertEqual(
            actual_writer_config["stream_options"],
            self.mock_config['stream_options']
        )
        self.assertEqual(
            actual_writer_config['writer_all_options']['output_mode'],
            self.mock_config['outputMode']
        )
        self.assertEqual(
            actual_writer_config['writer_all_options']['write_options'],
            {
                "kafka.bootstrap.servers": "test-server",
                "kafka.security.protocol": "test-SSL",
                "topic": "test-topic",
                "kafka.ssl.keystore.location": "/path/test-key.jks",
                "kafka.ssl.keystore.password": "test-key-store-password",
                "kafka.ssl.truststore.location": "/path/test-trust.jks",
                "kafka.ssl.truststore.password": "test-trust-store-password",
                "kafka.ssl.key.password": "test-ssl-key-password",
                "startingOffsets": "test-latest"
            }
        )

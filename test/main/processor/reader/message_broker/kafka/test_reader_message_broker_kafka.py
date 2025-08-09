"""
Test Message Broker Kafka Reader Implementation Class
"""
import unittest
from unittest.mock import MagicMock, patch

from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
)

from src.main.processor.reader.message_broker.kafka.reader_message_broker_kafka import (
    ReaderMessageBrokerKafka,
)
from src.main.utils.json_utils import (
    StructDict,
)


# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=too-few-public-methods
# pylint: disable=pointless-string-statement

class TestReaderMessageBrokerKafka(unittest.TestCase):
    """
    Test File System Reader Implementation Class)
    """

    def setUp(self):
        """
        Set up the test environment.
        """
        self.mock_spark = MagicMock()
        self.mock_logger = MagicMock()
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
            "input": {
                "reader_typ": "test_message_broker_kafka",
                "format": "",
                "secret_name": "test_secret_name",
                "format_options": {
                    "kafka.bootstrap.servers": "test-server",
                    "kafka.security.protocol": "test-SSL",
                    "subscribe": "test-topic",
                    "kafka.ssl.keystore.location": "/path/test-key.jks",
                    "kafka.ssl.keystore.password": "aws_sm_key_store_password",
                    "kafka.ssl.truststore.location": "/path/test-trust.jks",
                    "kafka.ssl.truststore.password": "aws_sm_trust_store_password",
                    "kafka.ssl.key.password": "aws_sm_ssl_key_password",
                    "startingOffsets": "test-latest",
                    "failOnDataLoss": "test-false"
                },
                "schema": {
                    "type": "string"
                }
            }
        }
        self.mock_schema = StructType([
            StructField("dummy", IntegerType(), True)
        ])
        self.mock_secret_instance = MagicMock()

    @patch("src.main.processor.reader.message_broker.kafka.reader_message_broker_kafka.generate_spark_schema_from_json")
    @patch("src.main.processor.reader.message_broker.kafka.reader_message_broker_kafka.generate_spark_schema_from_avro")
    @patch("src.main.processor.reader.message_broker.kafka.reader_message_broker_kafka.SecretUtils")
    def test_reader_get_json_format(self, mock_secret_utils, mock_generate_spark_schema_from_avro,
                                    mock_generate_spark_schema_from_json):
        """
        Test the reader_get method for JSON format.
        :param mock_generate_spark_schema_from_json:
        :param mock_generate_spark_schema_from_avro:
        :return:
        """
        mock_secret_utils.return_value = self.mock_secret_instance
        self.mock_secret_instance.get_secret.return_value = self.mock_secret_instance_return_value
        mock_generate_spark_schema_from_json.return_value = self.mock_schema
        self.mock_config['input']['format'] = 'json'
        config = StructDict(**self.mock_config)
        reader = ReaderMessageBrokerKafka(self.mock_spark, config, self.mock_logger)
        actual_result: dict = reader.reader_get()

        for called_args, called_kwargs in mock_generate_spark_schema_from_json.call_args_list:
            actual_arg = dict(called_args[0])
            self.assertEqual(actual_arg, {'type': 'string'})

        mock_generate_spark_schema_from_avro.assert_not_called()

        self.assertEqual(actual_result["format"], "json")
        self.assertEqual(actual_result["options"], {
            "kafka.bootstrap.servers": "test-server",
            "kafka.security.protocol": "test-SSL",
            "subscribe": "test-topic",
            "kafka.ssl.keystore.location": "/path/test-key.jks",
            "kafka.ssl.keystore.password": "test-key-store-password",
            "kafka.ssl.truststore.location": "/path/test-trust.jks",
            "kafka.ssl.truststore.password": "test-trust-store-password",
            "kafka.ssl.key.password": "test-ssl-key-password",
            "startingOffsets": "test-latest",
            "failOnDataLoss": "test-false"
        })
        self.assertEqual(actual_result["schema"], self.mock_schema)
        self.assertEqual(actual_result["spark"], self.mock_spark)

    @patch("src.main.processor.reader.message_broker.kafka.reader_message_broker_kafka.generate_spark_schema_from_json")
    @patch("src.main.processor.reader.message_broker.kafka.reader_message_broker_kafka.generate_spark_schema_from_avro")
    @patch("src.main.processor.reader.message_broker.kafka.reader_message_broker_kafka.SecretUtils")
    def test_reader_get_avro_format(self, mock_secret_utils, mock_generate_spark_schema_from_avro,
                                    mock_generate_spark_schema_from_json):
        """
        Test the reader_get method for AVRO format.
        :param mock_generate_spark_schema_from_json:
        :param mock_generate_spark_schema_from_avro:
        :return:
        """
        mock_secret_utils.return_value = self.mock_secret_instance
        self.mock_secret_instance.get_secret.return_value = self.mock_secret_instance_return_value
        mock_generate_spark_schema_from_avro.return_value = self.mock_schema
        self.mock_config['input']['format'] = 'avro'
        self.mock_config['input']['format_options']['starting_offset'] = 'earliest'
        config = StructDict(**self.mock_config)
        reader = ReaderMessageBrokerKafka(self.mock_spark, config, self.mock_logger)
        actual_result: dict = reader.reader_get()

        for called_args, called_kwargs in mock_generate_spark_schema_from_avro.call_args_list:
            actual_arg = dict(called_args[0])
            self.assertEqual(actual_arg, {'type': 'string'})

        mock_generate_spark_schema_from_json.assert_not_called()

        self.assertEqual(actual_result["format"], "avro")
        self.assertEqual(actual_result["options"], {
            "kafka.bootstrap.servers": "test-server",
            "kafka.security.protocol": "test-SSL",
            "subscribe": "test-topic",
            "kafka.ssl.keystore.location": "/path/test-key.jks",
            "kafka.ssl.keystore.password": "test-key-store-password",
            "kafka.ssl.truststore.location": "/path/test-trust.jks",
            "kafka.ssl.truststore.password": "test-trust-store-password",
            "kafka.ssl.key.password": "test-ssl-key-password",
            "startingOffsets": "test-latest",
            "failOnDataLoss": "test-false"
        })
        self.assertEqual(actual_result["schema"], self.mock_schema)
        self.assertEqual(actual_result["spark"], self.mock_spark)

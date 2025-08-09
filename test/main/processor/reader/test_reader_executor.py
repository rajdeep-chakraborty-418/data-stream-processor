"""
Test Reader Entrypoint Methods
"""
# pylint: disable=line-too-long
# pylint: disable=import-error
# pylint: disable=too-few-public-methods
# pylint: disable=unnecessary-pass

import unittest
from unittest.mock import (
    patch,
    MagicMock,
)
from src.main.processor.reader.reader_executor import (
    reader_executor,
    reader_stream_executor,
)


class TestReaderExecutor(unittest.TestCase):
    """
    Test Class to Call the Appropriate Child Reader Method and get the streaming dataframe
    """

    def setUp(self):
        """
        Set up the test environment, including mock objects.
        :return:
        """
        self.mock_spark = MagicMock()
        self.mock_logger = MagicMock()
        self.mock_config = MagicMock()
        self.mock_df = MagicMock(name='DataFrame')

    @patch("src.main.processor.reader.reader_executor.get_deep_attributes")
    @patch("src.main.processor.reader.reader_executor.ReaderFileSystem")
    def test_reader_executor_file_system(self, mock_ReaderFileSystem,
                                         mock_get_deep_attributes):
        """
        Test the reader_executor function for file_system reader type.
        :param mock_ReaderFileSystem:
        :param mock_get_deep_attributes:
        :return:
        """
        mock_get_deep_attributes.return_value = "file_system"
        mock_reader_instance = MagicMock()
        mock_ReaderFileSystem.return_value = mock_reader_instance
        mock_schema = MagicMock()
        reader_typ_options = {
            "spark": self.mock_spark,
            "format": "json",
            "options": {
                "sample_option": "value"
            },
            "schema": mock_schema,
            "path": "/data/"
        }
        expected_file_reader_options = {
            "reader_typ": "file_system",
            "reader_options": reader_typ_options
        }

        mock_reader_instance.reader_get.return_value = reader_typ_options
        actual_result = reader_executor(self.mock_spark, self.mock_config, self.mock_logger)

        mock_get_deep_attributes.assert_called_once_with(
            self.mock_config.input, ["reader_typ"]
        )
        mock_ReaderFileSystem.assert_called_once_with(
            self.mock_spark,
            self.mock_config
        )
        mock_reader_instance.reader_get.assert_called_once()
        self.mock_logger.info.assert_any_call(
            "Reader Implementation Module file_system will be invoked"
        )
        self.assertEqual(actual_result, expected_file_reader_options)

    @patch("src.main.processor.reader.reader_executor.get_deep_attributes")
    @patch("src.main.processor.reader.reader_executor.ReaderMessageBrokerKafka")
    def test_reader_executor_broker_message_kafka(self, mock_reader_message_broker_kafka, mock_get_deep_attributes):
        """
        Test the reader_executor function for file_system reader type.
        :param mock_reader_message_broker_kafka:
        :param mock_get_deep_attributes:
        :return:
        """
        mock_get_deep_attributes.return_value = "message_broker_kafka"
        mock_reader_instance = MagicMock()
        mock_reader_message_broker_kafka.return_value = mock_reader_instance
        mock_schema = MagicMock()
        reader_typ_options = {
            "spark": self.mock_spark,
            "format": "json",
            "options": {
                "sample_option": "sample_value"
            },
            "schema": mock_schema
        }
        expected_file_reader_options = {
            "reader_typ": "message_broker_kafka",
            "reader_options": reader_typ_options
        }

        mock_reader_instance.reader_get.return_value = reader_typ_options
        actual_result = reader_executor(self.mock_spark, self.mock_config, self.mock_logger)

        mock_get_deep_attributes.assert_called_once_with(
            self.mock_config.input, ["reader_typ"]
        )
        mock_reader_message_broker_kafka.assert_called_once_with(
            self.mock_spark,
            self.mock_config,
            self.mock_logger,
        )
        mock_reader_instance.reader_get.assert_called_once()
        self.mock_logger.info.assert_any_call(
            "Reader Implementation Module message_broker_kafka will be invoked"
        )
        self.assertEqual(actual_result, expected_file_reader_options)

    @patch('src.main.processor.reader.reader_executor.spark_read_file_system_stream')
    def test_reader_stream_executor_file_system(self, mock_spark_read_file_system_stream):
        """
        Test the reader_stream_executor function for file_system reader type.
        :param mock_spark_read_file_system_stream:
        :return:
        """
        mock_spark_read_file_system_stream.return_value = self.mock_df
        reader_typ = "file_system"
        reader_typ_options = {
            "spark": "mock_spark_session",
            "format": "json",
            "options": {
                "option1": "value1"
            },
            "schema": "mock_schema",
            "path": "/mock/path"
        }
        actual_result = reader_stream_executor(
            reader_typ,
            reader_typ_options
        )
        mock_spark_read_file_system_stream.assert_called_once_with(
            spark=reader_typ_options['spark'],
            file_format=reader_typ_options['format'],
            options=reader_typ_options['options'],
            schema=reader_typ_options['schema'],
            path=reader_typ_options['path']
        )
        self.assertEqual(actual_result, self.mock_df)

    @patch('src.main.processor.reader.reader_executor.spark_read_kafka_stream')
    @patch('src.main.processor.reader.reader_executor.format_kafka_json_dataframe')
    def test_reader_stream_executor_message_broker_kafka_json(self, mock_format_kafka_json_dataframe, mock_spark_read_kafka_stream):
        """
        Test the reader_stream_executor function for file_system reader type.
        :param mock_format_kafka_json_dataframe:
        :param mock_spark_read_kafka_stream:
        :return:
        """
        mock_format_kafka_json_dataframe.return_value = self.mock_df
        mock_spark_read_kafka_stream.return_value = self.mock_df

        reader_typ = "message_broker_kafka"
        reader_typ_options = {
            "spark": "mock_spark_session",
            "format": "json",
            "options": {
                "option-1": "value-1"
            },
            "schema": "mock_schema"
        }
        actual_result = reader_stream_executor(
            reader_typ,
            reader_typ_options
        )
        mock_spark_read_kafka_stream.assert_called_once_with(
            spark=reader_typ_options['spark'],
            options=reader_typ_options['options']
        )
        mock_format_kafka_json_dataframe.assert_called_once_with(
            input_dataframe=mock_spark_read_kafka_stream.return_value,
            input_schema=reader_typ_options['schema']
        )
        self.assertEqual(actual_result, self.mock_df)

    @patch('src.main.processor.reader.reader_executor.spark_read_kafka_stream')
    @patch('src.main.processor.reader.reader_executor.format_kafka_avro_dataframe')
    def test_reader_stream_executor_message_broker_kafka_avro(self, mock_format_kafka_avro_dataframe, mock_spark_read_kafka_stream):
        """
        Test the reader_stream_executor function for file_system reader type.
        :param mock_format_kafka_avro_dataframe:
        :param mock_spark_read_kafka_stream:
        :return:
        """
        mock_format_kafka_avro_dataframe.return_value = self.mock_df
        mock_spark_read_kafka_stream.return_value = self.mock_df

        reader_typ = "message_broker_kafka"
        reader_typ_options = {
            "spark": "mock_spark_session",
            "format": "avro",
            "options": {
                "option-1": "value-1"
            },
            "schema": "mock_schema"
        }
        actual_result = reader_stream_executor(
            reader_typ,
            reader_typ_options
        )
        mock_spark_read_kafka_stream.assert_called_once_with(
            spark=reader_typ_options['spark'],
            options=reader_typ_options['options']
        )
        mock_format_kafka_avro_dataframe.assert_called_once_with(
            input_dataframe=mock_spark_read_kafka_stream.return_value,
            input_schema=reader_typ_options['schema']
        )
        self.assertEqual(actual_result, self.mock_df)

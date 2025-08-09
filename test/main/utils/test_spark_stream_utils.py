"""
Test Streaming Utility Methods
"""
# pylint: disable=import-error
# pylint: disable=line-too-long

import unittest
from unittest.mock import (
    MagicMock,
)

from pyspark.sql import (
    SparkSession,
    DataFrame,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)

from src.main.utils.spark_stream_utils import (
    spark_read_file_system_stream, spark_read_kafka_stream,
)


class TestSparkStreamUtils(unittest.TestCase):
    """
    Test  Spark Streaming Utilities
    """

    def setUp(self):
        """
        Set up the test case with a mock Spark session and schema
        :return:
        """
        self.spark = MagicMock(SparkSession)
        self.schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
        self.options = {
            "option1": "value1",
            "option2": "value2"
        }
        self.path = "/mock/path"


    def test_spark_read_file_system_stream(self):
        """
        Test the spark_read_file_system_stream function
        :return:
        """
        read_stream_mock = MagicMock()
        self.spark.readStream = read_stream_mock

        df_mock = MagicMock(DataFrame)
        read_stream_mock.format.return_value = read_stream_mock
        read_stream_mock.schema.return_value = read_stream_mock
        read_stream_mock.options.return_value = read_stream_mock
        read_stream_mock.load.return_value = df_mock

        actual_dataframe = spark_read_file_system_stream(
            self.spark,
            "json",
            self.options,
            self.schema,
            self.path
        )

        self.spark.readStream.format.assert_called_once_with("json")
        self.spark.readStream.schema.assert_called_once_with(self.schema)
        self.assertEqual(self.spark.readStream.schema.call_count, 1)
        self.assertEqual(self.spark.readStream.options.call_count, 1)
        self.assertEqual(self.spark.readStream.options.call_args_list[0][1], self.options)
        self.spark.readStream.load.assert_called_once_with(self.path)
        self.assertEqual(actual_dataframe, df_mock)

    def test_spark_read_kafka_stream(self):
        """
        Test the spark_read_kafka_stream function
        :return:
        """
        mock_options = {
            "kafka.bootstrap.servers": "localhost:9092",
            "subscribe": "test_topic"
        }
        read_stream_mock = MagicMock()
        self.spark.readStream = read_stream_mock

        df_mock = MagicMock(name="DataFrame")
        df_mock_select = MagicMock(name="SelectDataFrame")

        read_stream_mock.format.return_value = read_stream_mock
        read_stream_mock.options.return_value = read_stream_mock
        read_stream_mock.load.return_value = df_mock
        df_mock.selectExpr.return_value = df_mock_select

        actual_result_df = spark_read_kafka_stream(
            self.spark,
            mock_options
        )

        read_stream_mock.format.assert_called_once_with("kafka")
        read_stream_mock.options.assert_called_once_with(**mock_options)
        read_stream_mock.load.assert_called_once()

        df_mock.selectExpr.assert_called_once_with(
            "CAST(key AS STRING) as event_key",
            "value as event_data"
        )
        self.assertEqual(actual_result_df, df_mock_select)

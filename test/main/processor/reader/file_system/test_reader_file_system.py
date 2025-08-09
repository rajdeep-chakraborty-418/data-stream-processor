"""
Test File System Reader Implementation Class
"""
# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=pointless-string-statement

import unittest
from unittest.mock import (
    MagicMock,
    patch,
)

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
)

from src.main.processor.reader.file_system.reader_file_system import (
    ReaderFileSystem,
)
from src.main.utils.json_utils import (
    StructDict,
)


class TestReaderFileSystem(unittest.TestCase):
    """
    Test File System Reader Implementation Class
    """

    @patch("src.main.processor.reader.file_system.reader_file_system.get_deep_attributes")
    @patch("src.main.processor.reader.file_system.reader_file_system.generate_spark_schema_from_json")
    @patch("src.main.processor.reader.file_system.reader_file_system.generate_spark_schema_from_avro")
    def test_reader_get_json_format(self, mock_generate_spark_schema_from_avro, mock_generate_spark_schema_from_json,
                                    mock_get_deep_attributes):
        """
        Test the reader_get method for JSON format.
        :param mock_get_deep_attributes:
        :param mock_generate_spark_schema_from_json:
        :param mock_generate_spark_schema_from_avro:
        :return:
        """
        mock_spark = MagicMock()
        config = MagicMock()
        """
        Simulate get_deep_attributes returning values in order
        """
        mock_get_deep_attributes.side_effect = [
            "json",
            StructDict(type="string"),
            "/input/path/",
            "*.json",
            StructDict(multiLine="true", timeline="false")
        ]
        mock_json_schema = StructType([
            StructField("dummy", IntegerType(), True)
        ])
        mock_generate_spark_schema_from_json.return_value = mock_json_schema

        reader = ReaderFileSystem(mock_spark, config)
        actual_result = reader.reader_get()

        for called_args, called_kwargs in mock_generate_spark_schema_from_json.call_args_list:
            actual_arg = dict(called_args[0])
            self.assertEqual(actual_arg, {'type': 'string'})

        mock_generate_spark_schema_from_avro.assert_not_called()

        self.assertEqual(actual_result["format"], "json")
        self.assertEqual(actual_result["path"], "/input/path/*.json")
        self.assertEqual(actual_result["schema"], mock_json_schema)
        self.assertEqual(actual_result["options"], {"multiLine": True, "timeline": False})
        self.assertEqual(actual_result["spark"], mock_spark)

    @patch("src.main.processor.reader.file_system.reader_file_system.get_deep_attributes")
    @patch("src.main.processor.reader.file_system.reader_file_system.generate_spark_schema_from_json")
    @patch("src.main.processor.reader.file_system.reader_file_system.generate_spark_schema_from_avro")
    def test_reader_get_avro_format(self, mock_generate_spark_schema_from_avro, mock_generate_spark_schema_from_json,
                                    mock_get_deep_attributes):
        """
        Test the reader_get method for JSON format.
        :param mock_get_deep_attributes:
        :param mock_generate_spark_schema_from_json:
        :param mock_generate_spark_schema_from_avro:
        :return:
        """
        mock_spark = MagicMock()
        config = MagicMock()

        mock_get_deep_attributes.side_effect = [
            "avro",
            StructDict(fields=[]),
            "/input/path/",
            "data-*.avro",
            StructDict(someOption="false", option="value")
        ]
        mock_avro_schema = StructType([
            StructField("dummy", IntegerType(), True)
        ])
        mock_generate_spark_schema_from_avro.return_value = mock_avro_schema

        reader = ReaderFileSystem(mock_spark, config)
        result = reader.reader_get()

        for called_args, called_kwargs in mock_generate_spark_schema_from_avro.call_args_list:
            actual_arg = dict(called_args[0])
            self.assertEqual(actual_arg, {'fields': []})
        mock_generate_spark_schema_from_json.assert_not_called()

        self.assertEqual(result["format"], "avro")
        self.assertEqual(result["path"], "/input/path/data-*.avro")
        self.assertEqual(result["schema"], mock_avro_schema)
        self.assertEqual(result["options"], {"someOption": False, "option": "value"})
        self.assertEqual(result["spark"], mock_spark)

"""
Test Databricks Volume Read Utilities
"""
# pylint: disable=import-error

import json
import unittest
from unittest.mock import (
    patch,
    mock_open,
)

from src.main.utils.databricks_utils.volume_read_utils import (
    read_from_volume,
)


class TestVolumeReadUtils(unittest.TestCase):
    """
    Test Utility to read objects from Volumes
    """
    @patch('builtins.open', new_callable=mock_open, read_data='{"key": "value"}')
    def test_read_json_file_from_volume(self, mock_file):
        """
        Test the __read_json_file method
        :param mock_file:
        :return:
        """
        file_path = "dummy_path.json"
        file_type = "json"

        result = read_from_volume(file_path, file_type)
        mock_file.assert_called_once_with(file_path, 'r', encoding='utf-8')
        self.assertEqual(result, {"key": "value"})

    @patch('builtins.open', new_callable=mock_open, read_data='some sql statements')
    def test_read_sql_file_from_volume(self, mock_file):
        """
        Test the __read_sql_file method
        :param mock_file:
        :return:
        """
        file_path = "dummy_path.sql"
        file_type = "sql"

        result = read_from_volume(file_path, file_type)
        mock_file.assert_called_once_with(file_path, 'r', encoding='utf-8')
        self.assertEqual(result, 'some sql statements')

    @patch('builtins.open', new_callable=mock_open, read_data='some pyspark statements')
    def test_read_pyspark_file_from_volume(self, mock_file):
        """
        Test the __read_pyspark_file method
        :param mock_file:
        :return:
        """
        file_path = "dummy_path.py"
        file_type = "pyspark"

        result = read_from_volume(file_path, file_type)
        mock_file.assert_called_once_with(file_path, 'r', encoding='utf-8')
        self.assertEqual(result, 'some pyspark statements')

    @patch('builtins.open', new_callable=mock_open)
    def test_read_file_not_found(self, mock_file):
        """
        Test the __read_file method when the file is not found
        :param mock_file:
        :return:
        """
        file_path = "non_existent_file.json"
        file_type = "json"
        with self.assertRaises((FileNotFoundError, json.JSONDecodeError)):
            read_from_volume(file_path, file_type)

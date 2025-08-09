"""
Test Common functions across Reader, Writer, Transformer Modules
"""

# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=pointless-string-statement

import os
import unittest
from unittest.mock import patch

from src.main.utils.processor_utils import (
    read_environment_var,
    read_config,
)


class TestReusableConfigUtils(unittest.TestCase):
    """
    Test Class to encapsulate all common functions across reader, writer, transformer modules
    """
    @patch("src.main.utils.processor_utils.read_from_volume")
    @patch("src.main.utils.processor_utils.read_from_s3")
    def test_read_config_from_volume(self, mock_read_from_s3, mock_read_from_volume):
        """
        Test Read Config Invoke for Volume
        :param mock_read_from_s3:
        :param mock_read_from_volume:
        :return:
        """
        mock_output = {
            "key": "value"
        }
        mock_read_from_volume.return_value = mock_output
        result = read_config(
            input_region="us-east-1",
            input_file_path="/mnt/config.json",
            input_file_typ="json",
            input_config_reader_source="databricks_volume"
        )
        mock_read_from_volume.assert_called_once_with(
            file_path="/mnt/config.json",
            file_typ="json"
        )
        self.assertEqual(result, mock_output)
        mock_read_from_s3.assert_not_called()

    @patch("src.main.utils.processor_utils.read_from_volume")
    @patch("src.main.utils.processor_utils.read_from_s3")
    def test_read_config_from_s3(self, mock_read_from_s3, mock_read_from_volume):
        """
        Test Read Config Invoke for S3
        :param mock_read_from_s3:
        :param mock_read_from_volume:
        :return:
        """
        mock_output = {"key": "value"}
        mock_read_from_s3.return_value = mock_output
        result = read_config(
            input_region="us-west-2",
            input_file_path="s3://test-bucket/config.json",
            input_file_typ="json",
            input_config_reader_source="s3"
        )
        mock_read_from_s3.assert_called_once_with(
            region_name="us-west-2",
            file_path="s3://test-bucket/config.json",
            file_typ="json"
        )
        self.assertEqual(result, mock_output)
        mock_read_from_volume.assert_not_called()

    @patch.dict(os.environ, {
        "CONFIG_FILE_PATH": "/mnt/config.json",
        "REGION": "us-west-2",
        "CONFIG_READER_SOURCE": "databricks_volume"
    })
    def test_read_environment_var(self):
        result = read_environment_var()
        expected = {
            "CONFIG_FILE_PATH": "/mnt/config.json",
            "REGION": "us-west-2",
            "CONFIG_READER_SOURCE": "databricks_volume"
        }
        self.assertEqual(result, expected)

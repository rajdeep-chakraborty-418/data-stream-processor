"""
S3 Utilities in AWS
"""

# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=pointless-string-statement

import unittest
from unittest.mock import (
    patch,
    MagicMock,
)

from src.main.utils.aws_utils.s3_utils import (
    S3Utils,
    read_from_s3,
)


class TestS3Utils(unittest.TestCase):
    """
    Test S3 Utilities Class
    """

    def setUp(self):
        """
        Test Initialization constructor params
        :return:
        """
        self.region_name = "test-region"
        self.s3_utils = S3Utils(region_name=self.region_name)
        self.valid_s3_path = "s3://test-bucket/test-path/to/file.json"
        self.invalid_s3_path = "https://test-bucket/test-path/to/file.json"
        self.invalid_s3_path_missing_bucket = "s3:///missingbucket"
        self.invalid_s3_path_missing_key = "s3://bucketonly/"
        self.mock_s3_client = MagicMock()

    def test_parse_s3_path_valid(self):
        """
        Test Parse of S3 into Bucket & Key pair
        :return:
        """
        bucket, key = self.s3_utils._S3Utils__parse_s3_path(self.valid_s3_path)
        self.assertEqual(bucket, "test-bucket")
        self.assertEqual(key, "test-path/to/file.json")

    def test_parse_s3_path_invalid_prefix(self):
        """
        Test Parse Failure of S3 into Bucket & Key pair
        :return:
        """
        with self.assertRaises(ValueError) as context:
            self.s3_utils._S3Utils__parse_s3_path(self.invalid_s3_path)

        self.assertIn(f"""Invalid S3 path {self.invalid_s3_path}. It must start with 's3://'""", str(context.exception))

    def test_parse_s3_path_missing_bucket_or_key(self):
        """
        Test Parse Failure of S3 Due to Missing Bucket or Missing Key
        :return:
        """
        with self.assertRaises(ValueError):
            self.s3_utils._S3Utils__parse_s3_path(self.invalid_s3_path_missing_bucket)

        with self.assertRaises(ValueError):
            self.s3_utils._S3Utils__parse_s3_path(self.invalid_s3_path_missing_key)

    @patch("src.main.utils.aws_utils.s3_utils.boto3.client")
    def test_read_file_success(self, mock_boto_client):
        """
        Test Read Json File with Success
        :param mock_boto_client:
        :return:
        """
        mock_boto_client.return_value = self.mock_s3_client
        mock_s3_utils = S3Utils(region_name=self.region_name)
        mock_responses = {
            "json": b'{"key": "value"}',
            "sql": b"select sql",
            "pyspark": b"execute pyspark script"
        }
        mock_results = {
            "json": {'key': 'value'},
            "sql": "select sql",
            "pyspark": "execute pyspark script"
        }
        for file_typ, content_bytes in mock_responses.items():
            with self.subTest(file_type=file_typ):
                mock_response = {
                    "Body": MagicMock(read=MagicMock(return_value=content_bytes))
                }
                self.mock_s3_client.get_object.return_value = mock_response
                actual_content = mock_s3_utils.get_file(
                    file_path=self.valid_s3_path,
                    file_typ=file_typ
                )
                self.assertEqual(actual_content, mock_results[file_typ])

    @patch("src.main.utils.aws_utils.s3_utils.S3Utils")
    def test_read_s3_file(self, mock_s3_utils):
        """
        Test External Public Method for Read from S3
        :param mock_s3_utils:
        :return:
        """
        self.mock_s3_client.get_file.return_value = "Some Return"
        mock_s3_utils.return_value = self.mock_s3_client
        actual_result = read_from_s3(
            region_name="test-region",
            file_typ="test-type",
            file_path="dummy-loc"
        )
        self.assertEqual(actual_result, "Some Return")

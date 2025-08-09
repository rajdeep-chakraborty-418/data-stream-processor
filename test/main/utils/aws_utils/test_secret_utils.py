"""
Test Secrets retrieval from AWS
"""

# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=pointless-string-statement

import unittest
from unittest.mock import (
    patch,
    MagicMock,
)
from botocore.exceptions import ClientError
import json

from src.main.utils.aws_utils.secret_utils import (
    SecretUtils,
)


class TestSecretUtils(unittest.TestCase):
    """
    Unit tests for the SecretUtils class that retrieves secrets from AWS Secrets Manager.
    """

    def setUp(self):
        """
        Set up the test environment with mock values for AWS region, secret name, and secret value.
        :return:
        """
        self.region = "mock-region"
        self.secret_name = "mock-secret"
        self.secret_value = {
            "username": "test-user",
            "password": "test-pass"
        }
        self.mock_logger = MagicMock()
        # self.secret_utils = SecretUtils(
        #     region_name=self.region, logger=self.mock_logger
        # )

    @patch("boto3.client")
    def test_get_secret_success(self, mock_boto_client):
        """
        Test the get_secret method to ensure it retrieves the secret successfully.
        :param mock_boto_client:
        :return:
        """
        mock_client_instance = MagicMock()
        mock_boto_client.return_value = mock_client_instance
        mock_client_instance.get_secret_value.return_value = {
            "SecretString": json.dumps(
                self.secret_value
            )
        }
        secret_utils = SecretUtils(region_name=self.region, logger=self.mock_logger)
        secret_utils.client = mock_client_instance

        result = secret_utils.get_secret(self.secret_name)
        self.assertEqual(result, self.secret_value)
        mock_client_instance.get_secret_value.assert_called_once_with(
            SecretId=self.secret_name
        )

    @patch("boto3.client")
    def test_get_secret_raises_client_error(self, mock_boto_client):
        """
        Test the get_secret method to ensure it raises a ClientError when the secret is not found.
        :param mock_boto_client:
        :return:
        """
        mock_client_instance = MagicMock()
        mock_boto_client.return_value = mock_client_instance
        error = ClientError(
            error_response={
                "Error": {
                    "Code": "ResourceNotFoundException", "Message": "Secret not found"
                }
            },
            operation_name="GetSecretValue"
        )
        mock_client_instance.get_secret_value.side_effect = error

        secret_utils = SecretUtils(
            region_name=self.region,
            logger=self.mock_logger
        )
        secret_utils.client = mock_client_instance

        with self.assertRaises(ClientError):
            secret_utils.get_secret(self.secret_name)

        self.mock_logger.error.assert_called_once()
        call_log_args = self.mock_logger.error.call_args[0][0]
        self.assertIn(f"Failed to get secret {self.secret_name} from AWS Secrets Manager", call_log_args)
        self.assertIn(self.secret_name, call_log_args)

"""
Secrets retrieval from AWS
"""

# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=pointless-string-statement

import json

import boto3
from botocore.exceptions import ClientError


class SecretUtils:
    """
    A utility class for retrieving secrets from AWS Secrets Manager.
    """
    def __init__(self, region_name, logger=None):
        """
        Initializes the SecretUtils class with the secret name and AWS region.
        """
        self.logger = logger
        self.client = boto3.client(
            service_name="secretsmanager",
            region_name=region_name
        )

    def get_secret(self, secret_name: str) -> dict:
        """
        Retrieves the secrets from AWS Secrets Manager
        :param self:
        :param secret_name:
        return
        """
        try:
            get_secret_value_response = self.client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            self.logger.error(f"""Failed to get secret {secret_name} from AWS Secrets Manager. Error: {e}""")
            raise

        return json.loads(get_secret_value_response["SecretString"])

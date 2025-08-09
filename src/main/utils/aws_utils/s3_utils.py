"""
S3 Utilities in AWS
"""
from typing import Any

# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=pointless-string-statement


import boto3
from botocore.exceptions import ClientError
from urllib.parse import (
    urlparse,
)

from src.main.enum.config_file_type import (
    ConfigFileType,
)
from src.main.utils.json_utils import (
    convert_string_to_json,
)


def read_from_s3(region_name: str, file_path: str, file_typ: str) -> Any:
    """
    External Public Method for Read from S3
    :param region_name:
    :param file_path:
    :param file_typ:
    :return:
    """
    s3_utils = S3Utils(
        region_name=region_name
    )
    return s3_utils.get_file(
        file_path=file_path,
        file_typ=file_typ
    )


class S3Utils:
    """
    Class to encapsulate all S3 utilities
    """

    def __init__(self, region_name: str):
        """
        Initialization script for S3 Utils
        :param: region_name
        """
        self.region_name = region_name
        self.s3_client = boto3.client('s3', region_name=self.region_name)

    @staticmethod
    def __parse_s3_path(s3_path):
        """
        Parses an S3 path (s3://bucket-name/path/to/file) into bucket name and key.
        :param s3_path: Absolute S3 path (e.g., 's3://bucket-name/path/to/file.json').
        :return: Tuple of (bucket_name, key).
        :raises: ValueError if the S3 path is invalid.
        """
        if not s3_path.startswith("s3://"):
            raise ValueError(f"""Invalid S3 path {s3_path}. It must start with 's3://'""")

        parsed_url = urlparse(s3_path)
        """
        Extract the Bucket Name
        """
        bucket_name = parsed_url.netloc
        """
        Get the File Key and remove the left /
        """
        file_key = parsed_url.path.lstrip('/')

        if not bucket_name or not file_key:
            raise ValueError(f"""Invalid S3 path {s3_path}. Could not extract bucket name or key""")

        return bucket_name, file_key

    def __read_file(self, bucket_name, key_name):
        """
        Read Json file
        :param bucket_name
        :param key_name
        :return:
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key_name)
            file_content = response['Body'].read().decode('utf-8')

        except ClientError as e:
            raise ValueError(
                f"""Failed to fetch file {key_name} from S3 Bucket {bucket_name}. Error: {e.response['Error']['Message']}""")

        except Exception as e:
            raise ValueError(
                f"""Unhandled error occurred while reading file {key_name} from bucket {bucket_name}: {str(e)}""")

        return file_content

    def __set_file(self, file_path: str, file_typ: str):
        """
        Set the Content of the file based on file typ
        :param file_path:
        :param file_typ:
        :return:
        """
        out_file_content = None
        """
        Parse S3 and generate Bucket & Key
        """
        parse_s3_out: (str, str) = self.__parse_s3_path(
            s3_path=file_path
        )
        bucket_name = parse_s3_out[0]
        key_name = parse_s3_out[1]
        """
        Get File Content using Bucket & Key
        """
        file_content = self.__read_file(
            bucket_name=bucket_name,
            key_name=key_name
        )
        """
        Based On File Type Respective Conversion will be applied
        """
        match file_typ:

            case ConfigFileType.JSON.value:
                out_file_content = convert_string_to_json(
                    input_str=file_content
                )

            case ConfigFileType.SQL.value:
                out_file_content = file_content

            case ConfigFileType.PYSPARK.value:
                out_file_content = file_content

        return out_file_content

    def get_file(self, file_path: str, file_typ: str) -> Any:
        """
        Get the Content of the file based on file typ
        :param file_path:
        :param file_typ:
        :return:
        """
        return self.__set_file(
            file_path=file_path,
            file_typ=file_typ
        )

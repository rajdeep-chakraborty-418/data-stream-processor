"""
Reusable Modules across Reader, Writer, Transformer Modules
"""

# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=pointless-string-statement

import os

from src.main.enum.config_reader_system import (
    ConfigReaderSystem,
)
from src.main.utils.aws_utils.s3_utils import (
    read_from_s3,
)
from src.main.utils.constants import (
    ENV_VAR_CONFIG_READER_SOURCE,
    ENV_VAR_REGION,
    ENV_VAR_CONFIG_FILE_PATH,
)
from src.main.utils.databricks_utils.volume_read_utils import (
    read_from_volume,
)


def read_config(input_region: str, input_file_path: str, input_file_typ: str, input_config_reader_source: str) -> dict:
    """
    Read User Config From User Location Type (Databricks Volume, S3)
    :param input_region:
    :param input_file_path:
    :param input_file_typ:
    :param input_config_reader_source:
    :return:
    """
    output_config: dict = {}
    match input_config_reader_source:

        case ConfigReaderSystem.DATABRICKS_VOLUME.value:
            """
            Read Config From Databricks Volume
            """
            output_config = read_from_volume(
                file_path=input_file_path,
                file_typ=input_file_typ
            )
        case ConfigReaderSystem.S3.value:
            """
            Read Config From AWS S3
            """
            output_config = read_from_s3(
                region_name=input_region,
                file_path=input_file_path,
                file_typ=input_file_typ
            )

    return output_config


def read_environment_var() -> dict:
    """
    Read Environment Variables
    :return:
    """
    return {
        ENV_VAR_CONFIG_FILE_PATH: os.getenv(ENV_VAR_CONFIG_FILE_PATH),
        ENV_VAR_REGION: os.getenv(ENV_VAR_REGION),
        ENV_VAR_CONFIG_READER_SOURCE: os.getenv(ENV_VAR_CONFIG_READER_SOURCE)
    }

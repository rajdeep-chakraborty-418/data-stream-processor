"""
Define ConfigReaderSystem Enum for different Config Reader Type
"""
from enum import Enum


class ConfigReaderSystem(Enum):
    """
    Enum for Config Reader Types
    """
    S3 = "s3"
    DATABRICKS_VOLUME = "databricks_volume"

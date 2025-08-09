"""
Define ConfigFileType Enum for different File Type
"""
from enum import Enum


class ConfigFileType(Enum):
    """
    Enum for Config File Types
    """
    JSON = "json"
    PYSPARK = "pyspark"
    SQL = "sql"

"""
Define ReaderFileType Enum for different File Type reader implementations
"""
from enum import Enum


class ReaderFileType(Enum):
    """
    Enum for Reader Types
    """
    JSON = "json"
    AVRO = "avro"

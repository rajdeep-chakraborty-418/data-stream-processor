"""
Define WriterFileType Enum for different File Type reader implementations
"""
from enum import Enum


class WriterFileType(Enum):
    """
    Enum for Reader Types
    """
    JSON = "json"
    AVRO = "avro"

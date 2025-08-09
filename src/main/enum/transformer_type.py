"""
Define Transformer Types Enum
"""
from enum import Enum


class TransformerType(Enum):
    """
    Enum for Transformer Types
    """
    SQL = "sql"
    PYSPARK = "pyspark"

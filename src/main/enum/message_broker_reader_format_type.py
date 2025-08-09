"""
Message Broker Data Format Type Enum
"""

from enum import Enum


class MessageBrokerReaderFormatType(Enum):
    """
    Enum for Reader Types
    """
    JSON = "json"
    AVRO = "avro"

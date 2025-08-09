"""
Define ReaderType Enum for different high level reader implementations
"""
from enum import Enum


class ReaderType(Enum):
    """
    Enum for Reader Types
    """
    FILE_SYSTEM = "file_system"
    MESSAGE_BROKER_KAFKA = "message_broker_kafka"

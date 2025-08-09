"""
Define WriterType Enum for different high level reader implementations
"""
from enum import Enum


class WriterType(Enum):
    """
    Enum for Writer Types
    """
    FILE_SYSTEM = "file_system"
    MESSAGE_BROKER_KAFKA = "message_broker_kafka"

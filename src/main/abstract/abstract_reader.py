"""
This is Reader Parent Class Implementation
"""
# pylint: disable=unnecessary-pass
# pylint: disable=too-few-public-methods
# pylint: disable=line-too-long

from abc import (
    abstractmethod,
    ABC,
)

from pyspark.sql import (
    DataFrame,
)


class AbstractReader(ABC):
    """
    Reader Parent Class
    """
    def __init__(self, spark, config):
        """
        Initialize the Reader Class
        :param spark:
        :param config:
        """
        self.spark = spark
        self.aws_region = config.aws_region
        self.processor_typ = config.processor_typ
        self.logger_name = config.logger_name
        self.input_config = config.input

    @abstractmethod
    def reader_get(self) -> DataFrame:
        """
        Abstract Method, implementation class will be responsible for putting the logic
        :return:
        """

    @abstractmethod
    def _reader_set(self) -> dict:
        """
        Abstract Method, Set the all parameters for the Reader, implementation class will be responsible for putting the logic
        :return:
        """
    @abstractmethod
    def _reader_get_format(self) -> dict:
        """
        Abstract Method, Get the File Format for the Reader, implementation class will be responsible for putting the logic
        :return:
        """

    @abstractmethod
    def _reader_get_options(self) -> dict:
        """
        Abstract Method, Get the Options for the Reader, implementation class will be responsible for putting the logic
        :return:
        """

    @abstractmethod
    def _reader_get_schema(self) -> dict:
        """
        Abstract Method, Get the Schema for the Reader, implementation class will be responsible for putting the logic
        :return:
        """

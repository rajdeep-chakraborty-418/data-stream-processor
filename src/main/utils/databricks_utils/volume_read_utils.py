"""
Databricks Volume Read Utilities
"""
# pylint: disable=too-few-public-methods
# pylint: disable=pointless-string-statement
# pylint: disable=import-error

import json

from src.main.enum.config_file_type import (
    ConfigFileType,
)


def read_from_volume(file_path: str, file_typ: str):
    """
    External Public Method for Read from Volume
    :param file_path:
    :param file_typ:
    :return:
    """
    volume_read = VolumeReadUtils(file_path, file_typ)
    return volume_read.read_file_type()


class VolumeReadUtils:
    """
    Utility to read objects from Volumes
    """

    def __init__(self, file_path: str, file_typ: str):
        """
        Constructor Initialisation
        :param: file_typ
        :param: file_path
        """
        self.file_typ = file_typ
        self.file_path = file_path

    def __read_file(self):
        """
        Read file from Volume
        :return:
        """
        with open(self.file_path, 'r', encoding='utf-8') as file:
            file_content = file.read()

        return file_content

    def __read_json_file(self):
        """
        Read a Json File
        :return:
        """
        file_content = self.__read_file()
        try:
            json_content = json.loads(file_content)
            return json_content

        except json.JSONDecodeError as exception:
            raise exception

    def __read_sql_file(self):
        """
        Read a SQL File
        :return:
        """
        file_content = self.__read_file()
        return file_content

    def __read_pyspark_file(self):
        """
        Read a PySpark File
        :return:
        """
        file_content = self.__read_file()
        return file_content

    def read_file_type(self):
        """
        Read File and invoke appropriate method based on file type
        :return:
        """
        """
        Call Appropriate Method to Read File based on File Type
        """
        match self.file_typ:

            case ConfigFileType.JSON.value:
                json_val = self.__read_json_file()
                return json_val

            case ConfigFileType.SQL.value:
                sql_val = self.__read_sql_file()
                return sql_val

            case ConfigFileType.PYSPARK.value:
                pyspark_val = self.__read_pyspark_file()
                return pyspark_val

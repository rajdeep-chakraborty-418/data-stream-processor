"""
File System Reader Model Class
"""

# pylint: disable=import-error

from src.main.utils.constants import (
    NODE_INPUT_GEN_SPARK,
    NODE_INPUT_GEN_SCHEMA,
    NODE_INPUT_GEN_FORMAT,
    NODE_INPUT_GEN_OPTIONS,
    NODE_INPUT_GEN_PATH,
)


class ReaderFileSystemModel:
    """
    Reader File System Model Class for return to caller
    """

    def __init__(self):
        """
        Constructor for Reader File System Model Class
        """
        self._data = {
            NODE_INPUT_GEN_SPARK: None,
            NODE_INPUT_GEN_SCHEMA: None,
            NODE_INPUT_GEN_FORMAT: None,
            NODE_INPUT_GEN_OPTIONS: None,
            NODE_INPUT_GEN_PATH: None
        }

    def __setitem__(self, key, value):
        """
        Set the value in the dictionary using the key
        :param key:
        :param value:
        :return:
        """
        self._data[key] = value

    def get_all_data(self) -> dict:
        """
        Expose the entire dictionary to the caller
        :return:
        """
        return self._data

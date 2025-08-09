"""
Json Utility Methods
"""

# pylint: disable=too-few-public-methods

from typing import Any
import json


class StructDict:
    """
    A class that provides dictionary-like behavior.
    """

    def __init__(self, **kwargs):
        """
        Initialize the StructDict with keyword arguments.
        :param kwargs:
        """
        for key, value in kwargs.items():
            if isinstance(value, dict):
                self.__dict__[key] = StructDict(**value)
            else:
                self.__dict__[key] = value

    def items(self):
        """
        Mimic dictionary retrieval like behaviour
        :return:
        """
        return self.__dict__.items()

    def struct_to_dict(self, struct: dict) -> dict:
        """
        Recursively converts a StructDict (or a dictionary with nested StructDicts)
        to a regular Python dictionary.
        """
        if isinstance(struct, StructDict):
            return {key: self.struct_to_dict(value) for key, value in struct.items()}

        return struct


def get_deep_attributes(obj: Any, attrs: list[str]) -> Any:
    """
    Dynamically Call with nested nodes to get nested config
    :param obj:
    :param attrs:
    :return:
    """
    for attr in attrs:
        obj = getattr(obj, attr)
    return obj


def convert_string_to_json(input_str: str) -> dict:
    """
    Convert content of file to a json
    :param: input_str
    :return:
    """
    return json.loads(input_str)

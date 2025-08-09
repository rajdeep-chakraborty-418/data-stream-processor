"""
Test Class for JSON Utils
"""
# pylint: disable=import-error

import json
import unittest

from src.main.utils.json_utils import (
    StructDict,
    get_deep_attributes,
    convert_string_to_json,
)


class TestStructDict(unittest.TestCase):
    """
    Test Class to verify dictionary-like behavior
    """

    def setUp(self):
        """
        Initialise Test Config
        :return:
        """
        self.nested_data: dict = {
            "input": {
                "source": "s3a://your-bucket/data/",
                "format": "json",
                "filePattern": "data-*.json",
                "options": {
                    "multiLine": "true",
                    "recursiveFileLookup": "true",
                    "maxFilesPerTrigger": 100
                }
            }
        }
        self.obj = StructDict(**self.nested_data)

    def test_nested_dict_initialization(self):
        """
        Test Single Level Structure
        :return:
        """
        self.assertIsInstance(self.obj.input, StructDict)
        self.assertEqual(self.obj.input.source, "s3a://your-bucket/data/")
        self.assertIsInstance(self.obj.input.options, StructDict)
        self.assertEqual(self.obj.input.options.multiLine, "true")
        self.assertEqual(len(self.obj.input.options.items()), 3)

    def test_attribute_error_on_missing_key(self):
        """
        Test Missing Key in StructDict
        :return:
        """
        with self.assertRaises(AttributeError):
            _ = self.obj.missing_key

    def test_nested_dict(self):
        """
        Test Nested Dictionary Conversion
        :return:
        """
        expected = {
            "multiLine": "true",
            "recursiveFileLookup": "true",
            "maxFilesPerTrigger": 100
        }
        result = self.obj.struct_to_dict(self.obj.input.options)
        self.assertEqual(result, expected)


class TestGetDeepAttributes(unittest.TestCase):
    """
    Test Class to verify Dynamically Call with nested nodes to get nested config
    """

    def setUp(self):
        """
        Initialise Test Config
        :return:
        """
        self.json_config = """
        {
            "database": {
                "connection": {
                    "host": "localhost",
                    "port": 5432
                },
                "name": "test_db"
            }
        }
        """
        json_config = json.loads(self.json_config)
        self.config = StructDict(**json_config)

    def test_get_nested_node(self):
        """
        Test Nested Node retrieval
        :return:
        """
        result = get_deep_attributes(self.config, ["database", "connection", "host"])
        self.assertEqual(result, "localhost")

    def test_get_empty_node(self):
        """
        Test Empty Node
        :return:
        """
        result = get_deep_attributes(self.config, [])
        self.assertEqual(result, self.config)


def test_convert_string_to_json():
    """
    Test Convert content of file to a json
    :return:
    """
    input_value = '{"key": "value"}'
    actual_json: dict = convert_string_to_json(
        input_str=input_value
    )
    assert actual_json == {'key': 'value'}

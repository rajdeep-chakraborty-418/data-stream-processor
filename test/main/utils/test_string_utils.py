"""
Test Classes for reusable String methods
"""
# pylint: disable=import-error
import unittest

from src.main.utils.string_utils import (
    convert_string_to_init_cap,
)


class TestStringUtils(unittest.TestCase):
    """
    Test CLass for StringUtils
    """
    def test_convert_string_to_init_cap_single_word(self):
        """
        Test with a single word without any separator
        :return:
        """
        result = convert_string_to_init_cap("hello", "_")
        self.assertEqual(result, "Hello")

    def test_convert_string_to_init_cap_multiple_words(self):
        """
        Test with multiple words separated by underscore
        """
        result = convert_string_to_init_cap("hello_world", "_")
        self.assertEqual(result, "HelloWorld")

    def test_convert_string_to_init_cap_empty_separator(self):
        """
        Test with empty separator, which should raise an error
        """
        with self.assertRaises(ValueError) as exception:
            convert_string_to_init_cap("hello world", "")
            self.assertEqual(str(exception.exception), "Separator cannot be empty")

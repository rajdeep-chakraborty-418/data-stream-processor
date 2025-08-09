"""
String Utility Methods
"""


def convert_string_to_init_cap(input_str: str, input_sep: str) -> str:
    """
    Convert Into InitCap separated by separator
    :param input_sep:
    :param input_str:
    :return:
    """
    return ''.join(word.capitalize() for word in input_str.split(input_sep))

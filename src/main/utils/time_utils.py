"""
Time Utility Methods
"""

# pylint: disable=pointless-string-statement
# pylint: disable=line-too-long
# pylint: disable=import-error

from datetime import datetime


def get_current_datetime() -> datetime:
    """
    Get current datetime
    :return: datetime object
    """
    return datetime.now()


def get_time_diff(start_time: datetime, end_time: datetime) -> dict:
    """
    Calculate difference between two datetime objects
    :param start_time: Start datetime
    :param end_time: End datetime
    """
    delta = end_time - start_time
    diff_seconds = delta.total_seconds()
    diff_milliseconds = int(diff_seconds * 1000)
    return {
        "seconds": diff_seconds,
        "milliseconds": diff_milliseconds
    }

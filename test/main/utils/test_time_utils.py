"""
Time Utility Methods
"""

# pylint: disable=pointless-string-statement
# pylint: disable=line-too-long
# pylint: disable=import-error

from datetime import datetime, timedelta
import time

from src.main.utils.time_utils import (
    get_current_datetime,
    get_time_diff,
)


class TestTimeUtils:
    """
    Test Time Utility Methods
    """

    def test_get_current_datetime_returns_datetime(self):
        """
        Test Get current datetime
        :return:
        """
        now = get_current_datetime()
        assert isinstance(now, datetime)

    def test_get_time_diff_correctness(self):
        """
        Test Calculate difference between two datetime objects
        :return:
        """
        test_start_datetime = datetime.now()
        time.sleep(2)
        test_end_datetime = datetime.now()
        diff = get_time_diff(test_start_datetime, test_end_datetime)

        assert diff["seconds"] > 1.9
        assert diff["seconds"] < 2.1
        assert diff["milliseconds"] > 1900
        assert 90 <= diff["milliseconds"] <= 2100
        assert abs(diff["seconds"] * 1000 - diff["milliseconds"]) < 1

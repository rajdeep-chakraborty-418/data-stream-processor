"""
Test Class for Logger Utils
"""
# pylint: disable=import-error
# pylint: disable=import-error

import logging
import unittest
import datetime
from unittest.mock import (
    patch,
    MagicMock,
)
from src.main.utils.logger_utils import (
    LoggerUtils,
    log_start,
    log_end,
)


class TestLoggerUtils(unittest.TestCase):
    """
    Test Logger Utils
    """

    def test_setup_logger(self):
        """
        Test Logger Initialization
        :return:
        """
        logger_name = "testLogger"
        logger = LoggerUtils.setup_logger(logger_name)
        self.assertEqual(logger.name, logger_name)
        self.assertTrue(logger.hasHandlers())
        self.assertEqual(logger.level, logging.INFO)

    def test_log_start_logs_correct_message(self):
        """
        Test method for Log start time of the process.
        :return:
        """
        mock_logger = MagicMock()
        start_time = log_start(mock_logger)
        self.assertIsInstance(start_time, datetime.datetime)
        mock_logger.info.assert_called_with(
            "Process started at: %s", start_time.strftime('%Y-%m-%d %H:%M:%S.%f')
        )

    @patch("datetime.datetime")
    def test_log_end_logs_correct_messages(self, mock_datatime):
        """
        Test method for Log End time of the process.
        :return:
        """
        mock_logger = MagicMock()
        start_time = datetime.datetime(2025, 5, 15, 10, 0, 0)
        end_time = datetime.datetime(2025, 5, 15, 10, 0, 2)
        duration = end_time - start_time
        formatted_end_time = end_time.strftime('%Y-%m-%d %H:%M:%S.%f')

        with patch('datetime.datetime.now', return_value=end_time):
            log_end(start_time, mock_logger)

        mock_logger.info.assert_any_call(
            f"Process ended at: %s", formatted_end_time
        )
        mock_logger.info.assert_any_call(
            f"Total Duration: {duration}"
        )

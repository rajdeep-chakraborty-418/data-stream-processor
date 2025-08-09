"""
Test Decorator Function with defined retry
"""

# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=pointless-string-statement

import unittest
from unittest.mock import (
    MagicMock, patch,
)

from src.main.utils.decorator.retry import (
    retry,
)


class TestRetryDecorator(unittest.TestCase):
    """
    Test Decorator Function with defined retry
    """

    def setUp(self):
        """
        Test Decorator Function with defined setup
        :return:
        """

    def test_retry_decorator_success_without_failure_retries(self):
        """
        Test Retry decorator with 1st time success
        :return:
        """
        logger = MagicMock()
        call_count = {"count": 0}

        @retry(exceptions=ValueError, logger=logger, tries=4, delay=1, backoff=2)
        def test_success_func():
            call_count["count"] += 1
            return "success"

        with patch("time.sleep") as mock_sleep:
            result = test_success_func()

        self.assertEqual(result, "success")
        self.assertEqual(call_count["count"], 1)
        self.assertEqual(logger.warning.call_count, 0)
        logger.warning.assert_not_called()
        mock_sleep.assert_not_called()

    def test_retry_decorator_success_after_failure_retries(self):
        """
        Test Retry decorator with exponential backoff with success after failure retries
        :return:
        """
        logger = MagicMock()
        call_count = {"count": 0}

        @retry(exceptions=ValueError, logger=logger, tries=4, delay=1, backoff=2)
        def test_success_func():
            call_count["count"] += 1
            if call_count["count"] < 4:
                raise ValueError("Fail")
            return "success"

        with patch("time.sleep") as mock_sleep:
            result = test_success_func()

        self.assertEqual(result, "success")
        self.assertEqual(call_count["count"], 4)
        self.assertEqual(logger.warning.call_count, 3)
        expected_calls = [
            unittest.mock.call(
                "Exception 'Fail' in 'test_success_func' Retrying in 1s... Attempts left: 3"
            ),
            unittest.mock.call(
                "Exception 'Fail' in 'test_success_func' Retrying in 2s... Attempts left: 2"
            ),
            unittest.mock.call(
                "Exception 'Fail' in 'test_success_func' Retrying in 4s... Attempts left: 1"
            ),
        ]
        logger.warning.assert_has_calls(expected_calls, any_order=False)
        mock_sleep.assert_has_calls(
            [unittest.mock.call(1), unittest.mock.call(2), unittest.mock.call(4)]
        )

    def test_retry_decorator_raises_after_all_tries(self):
        """
        Test Retry decorator with exponential backoff with failure after all retry
        :return:
        """
        logger = MagicMock()

        @retry(exceptions=ValueError, logger=logger, tries=3, delay=1, backoff=2)
        def test_fail_func():
            raise ValueError("Fail")

        with patch("time.sleep") as mock_sleep:
            with self.assertRaises(ValueError):
                test_fail_func()

        self.assertEqual(logger.warning.call_count, 3)
        expected_calls = [
            unittest.mock.call(
                "Exception 'Fail' in 'test_fail_func' Retrying in 1s... Attempts left: 2"
            ),
            unittest.mock.call(
                "Exception 'Fail' in 'test_fail_func' Retrying in 2s... Attempts left: 1"
            ),
            unittest.mock.call(
                "Exception 'Fail' in 'test_fail_func' Retrying in 4s... Attempts left: 0"
            )
        ]
        logger.warning.assert_has_calls(expected_calls, any_order=False)
        mock_sleep.assert_has_calls(
            [unittest.mock.call(1), unittest.mock.call(2)]
        )

    def test_retry_decorator_non_retryable_exception(self):
        """
        Test Retry decorator with exceptions shouldn't be retried
        :return:
        """
        logger = MagicMock()

        @retry(exceptions=ValueError, logger=logger, tries=3, delay=1, backoff=2)
        def no_retry_func():
            raise TypeError("No Retry")

        with self.assertRaises(TypeError):
            no_retry_func()

        logger.warning.assert_not_called()

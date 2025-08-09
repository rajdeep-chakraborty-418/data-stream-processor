"""
Test Utility To create Thread Pools for Async Operations
"""

# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=pointless-string-statement

import unittest
from unittest.mock import (
    patch, MagicMock,
)

from src.main.utils.thread_utils import (
    create_thread,
)


class TestThreadPool(unittest.TestCase):
    """
    Test Class For Thread Pools for Async Operations
    """
    @patch("src.main.utils.thread_utils.ThreadPoolExecutor")
    def test_create_thread(self, mock_thread_pool_executor):
        """
        Test Create Thread Pool Based on Worker Nodes and Thread Prefix
        :param mock_thread_pool_executor:
        :return:
        """
        mock_executor_instance = MagicMock()
        mock_thread_pool_executor.return_value = mock_executor_instance
        actual_executor = create_thread(
            input_max_worker=0,
            input_thread_name_prefix="test-thread"
        )
        mock_thread_pool_executor.assert_called_once_with(
            max_workers=0,
            thread_name_prefix="test-thread",
            initializer=None,
            initargs=()
        )
        self.assertEqual(actual_executor, mock_executor_instance)

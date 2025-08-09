"""
Utility To create Thread Pools for Async Operations
"""

# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=pointless-string-statement

from concurrent.futures import ThreadPoolExecutor
from typing import Any


def create_thread(input_max_worker: int, input_thread_name_prefix) -> Any:
    """
    Create Thread Pool Based on Worker Nodes and Thread Prefix
    :param input_max_worker:
    :param input_thread_name_prefix:
    :param
    :return:
    """
    return ThreadPoolExecutor(
        max_workers=input_max_worker,
        thread_name_prefix=input_thread_name_prefix,
        initializer=None,
        initargs=()
    )

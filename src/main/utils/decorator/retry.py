"""
Decorator Function with defined retry
"""

# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=pointless-string-statement

import time
import functools


def retry(exceptions, logger, tries: int, delay: int, backoff: int):
    """
    Retry decorator with exponential backoff.
    :param exceptions:
    :param tries:
    :param delay:
    :param backoff:
    :param logger:
    """
    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper_retry(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 0:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    _tries -= 1
                    logger.warning(
                        f"""Exception '{str(e)}' in '{func.__name__}' """
                        f"""Retrying in {_delay}s... Attempts left: {_tries}"""
                    )
                    if _tries == 0:
                        raise
                    time.sleep(_delay)
                    _delay *= backoff

        return wrapper_retry

    return decorator_retry

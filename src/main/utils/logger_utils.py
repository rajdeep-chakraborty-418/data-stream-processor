"""
Initiate a Logger Utils Instance
"""
# pylint: disable=too-few-public-methods

import logging
import datetime


class LoggerUtils:
    """
    Utility Methods for Logger Setup
    """

    @staticmethod
    def setup_logger(logger_name, logging_level=logging.INFO):
        """
        Initiate a Logger instance
        :param logger_name:
        :param logging_level:
        :return:
        """
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging_level)
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(name)s - %(levelname)s - %(message)s')
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        return logger


def log_start(logger):
    """
    Logs the start time of the process.
    :param logger:
    :return:
    """
    current_time_start = datetime.datetime.now()
    logger.info("Process started at: %s", current_time_start.strftime('%Y-%m-%d %H:%M:%S.%f'))
    return current_time_start


def log_end(start_time, logger):
    """
    Logs the end time of the process and calculates the duration.
    :param start_time:
    :param logger:
    """
    current_time_end = datetime.datetime.now()
    duration = current_time_end - start_time
    logger.info("Process ended at: %s", current_time_end.strftime('%Y-%m-%d %H:%M:%S.%f'))
    logger.info(f"Total Duration: {duration}")

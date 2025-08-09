"""
Splunk Metric Push using HTTP Event Collector
"""

# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=pointless-string-statement

import json
import os
import requests

from src.main.utils.decorator.retry import (
    retry,
)


def splunk_metric_retry_decorator(splunk_payload: dict, logger) -> None:
    """
    Invoke Metrics Push to Splunk with retry decorator
    :param splunk_payload:
    :param logger:
    :return:
    """
    logger.info(f"""Send Metrics To Splunk Decorator Started""")
    decorated_send_metrics_to_splunk = retry(
        (requests.RequestException,),
        tries=3,
        delay=1,
        backoff=2,
        logger=logger
    )(send_metrics_to_splunk)
    logger.info(f"""Send Metrics To Splunk Decorator Completed""")

    try:
        logger.info(f"""Send Metrics To Splunk Started""")
        decorated_send_metrics_to_splunk(
            splunk_payload=splunk_payload,
            logger=logger
        )
        logger.info("Send Metrics To Splunk Completed")
    except Exception as ex:
        logger.error(f"""Send Metrics To Splunk Failed: {str(ex)}""")


def send_metrics_to_splunk(splunk_payload: dict, logger):
    """
    Push metrics to Splunk
    :param splunk_payload:
    :param logger:
    :return:
    """
    splunk_hec_token = os.getenv("SPLUNK_HEC_TOKEN")
    splunk_hec_url = os.getenv("SPLUNK_HEC_URL")
    logger.info(f"""Environment Variable for Splunk Connection Retrieved""")
    headers = {
        "X-SF-Token": f"""{splunk_hec_token}""",
        "Content-Type": "application/json",
    }
    splunk_payload_json = json.dumps(splunk_payload)
    response = requests.post(
        splunk_hec_url,
        headers=headers,
        data=splunk_payload_json,
        timeout=5
    )
    logger.info(
        f"""Splunk Observability Cloud Response Code {str(response.status_code)} and Response Body {response.text}""")
    response.raise_for_status()

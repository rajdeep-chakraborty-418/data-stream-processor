"""
Test batches (chunks) from the metrics list.
"""
import json
# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=pointless-string-statement

import os
import unittest
from unittest.mock import (
    patch,
    MagicMock,
    call,
)

import requests


@patch.dict(os.environ, {
    "SPLUNK_HEC_URL": "http://fake-url",
    "SPLUNK_HEC_TOKEN": "fake-token"
})
class TestSplunkMetricRetryDecorator(unittest.TestCase):
    """
    Test Invoke Metrics Push to Splunk with retry decorator
    """

    def setUp(self):
        """
        Initialise common attributes
        :return:
        """
        self.mock_payload = {
            "gauge": [
                {"metric": "test-gauge", "value": 0.9}
            ],
            "counter": [
                {"metric": "test-counter", "value": 1}
            ]
        }

    @patch("time.sleep")
    @patch("requests.post")
    def test_decorator_invokes_send_metrics_success(self, mock_post, mock_sleep):
        """
        Test Invoke Metrics Push to Splunk with retry decorator with success
        :param mock_sleep:
        :return:
        """
        mock_logger = MagicMock()
        mock_logger = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"
        mock_response.raise_for_status = MagicMock()
        mock_post.side_effect = [
            mock_response
        ]
        from src.main.observability.splunk.http_event_collector.connector.metric_splunk_connector import \
            splunk_metric_retry_decorator
        splunk_metric_retry_decorator(
            splunk_payload=self.mock_payload,
            logger=mock_logger
        )
        self.assertEqual(mock_post.call_count, 1)
        self.assertEqual(mock_post.call_args_list[0][0][0], "http://fake-url")
        self.assertEqual(mock_post.call_args_list[0][1]['headers']['X-SF-Token'], "fake-token")
        self.assertEqual(mock_post.call_args_list[0][1]['data'], json.dumps(self.mock_payload))
        mock_sleep.assert_not_called()

    @patch("time.sleep")
    @patch("requests.post")
    def test_decorator_invokes_fails_retries_send_metrics_success(self, mock_post, mock_sleep):
        """
        Test Invoke Metrics Push to Splunk with retry decorator with failure and then success
        :param mock_sleep:
        :return:
        """
        mock_logger = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"
        mock_response.raise_for_status = MagicMock()
        mock_post.side_effect = [
            requests.RequestException("Fail 1"),
            requests.RequestException("Fail 2"),
            mock_response
        ]
        from src.main.observability.splunk.http_event_collector.connector.metric_splunk_connector import splunk_metric_retry_decorator
        splunk_metric_retry_decorator(
            splunk_payload=self.mock_payload,
            logger=mock_logger
        )
        self.assertEqual(mock_post.call_count, 3)
        self.assertEqual(mock_post.call_args_list[0][0][0], "http://fake-url")
        self.assertEqual(mock_post.call_args_list[0][1]['headers']['X-SF-Token'], "fake-token")
        self.assertEqual(mock_post.call_args_list[0][1]['data'], json.dumps(self.mock_payload))
        expected_warnings = [
            call("Exception 'Fail 1' in 'send_metrics_to_splunk' Retrying in 1s... Attempts left: 2"),
            call("Exception 'Fail 2' in 'send_metrics_to_splunk' Retrying in 2s... Attempts left: 1")
        ]
        mock_logger.warning.assert_has_calls(expected_warnings, any_order=False)
        mock_logger.info.assert_any_call(f"Send Metrics To Splunk Completed")
        mock_logger.error.assert_not_called()
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_has_calls(
            [unittest.mock.call(1), unittest.mock.call(2)]
        )

    @patch("time.sleep")
    @patch("requests.post")
    def test_decorator_invokes_fails_all_retries(self, mock_post, mock_sleep):
        """
        Test Invoke Metrics Push to Splunk with retry decorator with all failure
        :param mock_sleep:
        :return:
        """
        mock_logger = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"
        mock_response.raise_for_status = MagicMock()
        mock_post.side_effect = [
            requests.RequestException("Fail 1"),
            requests.RequestException("Fail 2"),
            requests.RequestException("Fail 3")
        ]
        from src.main.observability.splunk.http_event_collector.connector.metric_splunk_connector import splunk_metric_retry_decorator
        splunk_metric_retry_decorator(
            splunk_payload=self.mock_payload,
            logger=mock_logger
        )
        self.assertEqual(mock_post.call_count, 3)
        self.assertEqual(mock_post.call_args_list[0][0][0], "http://fake-url")
        self.assertEqual(mock_post.call_args_list[0][1]['headers']['X-SF-Token'], "fake-token")
        self.assertEqual(mock_post.call_args_list[0][1]['data'], json.dumps(self.mock_payload))
        expected_warnings = [
            call("Exception 'Fail 1' in 'send_metrics_to_splunk' Retrying in 1s... Attempts left: 2"),
            call("Exception 'Fail 2' in 'send_metrics_to_splunk' Retrying in 2s... Attempts left: 1"),
            call("Exception 'Fail 3' in 'send_metrics_to_splunk' Retrying in 4s... Attempts left: 0"),
        ]
        mock_logger.warning.assert_has_calls(expected_warnings, any_order=False)
        mock_logger.error.assert_any_call(f"Send Metrics To Splunk Failed: Fail 3")
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_has_calls(
            [unittest.mock.call(1), unittest.mock.call(2)]
        )

"""
Test Metric Builder Process
"""

# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=pointless-string-statement

import time
import unittest
from unittest.mock import (
    patch,
    MagicMock,
)
from src.main.observability.splunk.http_event_collector.builder.metric_builder import (
    MetricBuilder,
)


class TestMetricBuilderBuildMetricBuffer(unittest.TestCase):
    """
    This Class Validates functionality for
        __construct_metric
        build_metrics
    """

    def setUp(self):
        """
        Setup Initial Parameters
        :return:
        """
        self.static_vals = {
            "host": "test-host",
            "env": "test-env",
            "region": "test-region"
        }
        self.logger = MagicMock()

    @patch("time.sleep")
    @patch("src.main.observability.splunk.http_event_collector.builder.metric_builder.threading.Thread.start")
    def test_build_metrics_adds_to_correct_buffers(self, mock_thread, mock_sleep):
        """
        Test Method to Fill the Metrics values with dynamic values per microbatch
        Patching The Threading Module to Test
        :param: mock_thread
        :return:
        """
        mock_sleep.return_value = None
        mock_thread.return_value = None
        metric_builder = MetricBuilder(
            static_vals=self.static_vals,
            flush_interval_seconds=0,
            max_batch_size=0,
            logger=self.logger
        )
        gauge_metric = {
            "metric_type": "gauge",
            "metric": "test_gauge",
            "value": 55.0,
            "batch_id": -1
        }
        counter_metric = {
            "metric_type": "counter",
            "metric": "test_counter",
            "value": 100,
            "batch_id": -1
        }
        metric_builder.build_metrics([gauge_metric, counter_metric])
        self.assertEqual(len(metric_builder.buffer_gauge_metrics), 1)
        self.assertEqual(len(metric_builder.buffer_counter_metrics), 1)
        self.assertEqual(metric_builder.buffer_gauge_metrics[0]['metric'], 'test_gauge')
        self.assertEqual(metric_builder.buffer_gauge_metrics[0]['value'], 55.0)
        self.assertEqual(metric_builder.buffer_gauge_metrics[0]['dimensions'], {
            'host': 'test-host',
            'env': 'test-env',
            'region': 'test-region',
            'batch_id': -1,
        })
        self.assertEqual(metric_builder.buffer_counter_metrics[0]['metric'], 'test_counter')
        self.assertEqual(metric_builder.buffer_counter_metrics[0]['value'], 100)
        self.assertEqual(metric_builder.buffer_counter_metrics[0]['dimensions'], {
            'host': 'test-host',
            'env': 'test-env',
            'region': 'test-region',
            'batch_id': -1,
        })


class TestMetricBuilderPeriodicFlush(unittest.TestCase):
    """
    This Class Validates functionality for
        __periodic_flush
    """

    def setUp(self):
        """
        Setup Initial Parameters
        :return:
        """
        self.static_vals = {
            "host": "test-host",
            "env": "test-env",
            "region": "test-region"
        }
        self.logger = MagicMock()
        self.metric_builder = MetricBuilder(
            static_vals=self.static_vals,
            flush_interval_seconds=10,
            max_batch_size=2,
            logger=self.logger
        )
        self.metric_builder._check_interval = 0.01
        self.addCleanup(self.metric_builder.shutdown)

    def tearDown(self):
        """
        Stop the background thread
        :return:
        """
        self.metric_builder.shutdown()

    @patch("time.sleep")
    def test_periodic_flush_calls_flush_on_buffer_size_exceeded(self, mock_sleep):
        """
        Test Periodic Write to Splunk Based on Time or Batch Size
        :param mock_sleep:
        :return:
        """
        mock_sleep.return_value = None
        with patch.object(self.metric_builder, "flush_metrics_to_splunk") as mock_flush:
            self.metric_builder.buffer_gauge_metrics.extend([{"metric": "test"}] * 3)
            for _ in range(20):
                if mock_flush.called:
                    break
                time.sleep(0.1)
            self.metric_builder.shutdown()


class TestMetricBuilderShutdown(unittest.TestCase):
    """
    This Class Validates functionality for
        shutdown
    """

    def setUp(self):
        """
        Setup Initial Parameters
        :return:
        """
        self.static_vals = {
            "host": "test-host",
            "env": "test-env",
            "region": "test-region"
        }
        self.logger = MagicMock()
        self.metric_builder = MetricBuilder(
            static_vals=self.static_vals,
            flush_interval_seconds=5,
            max_batch_size=10,
            logger=self.logger
        )
        self.metric_builder._check_interval = 0.1

    def tearDown(self):
        """
        Teardown thread
        :return:
        """
        if self.metric_builder._flush_thread.is_alive():
            self.metric_builder.shutdown()

    @patch("time.sleep", return_value=None)
    @patch.object(MetricBuilder, "flush_metrics_to_splunk")
    def test_shutdown_stops_thread_and_logs(self, mock_flush, mock_sleep):
        """
        Test Shutdown Thread Gracefully in case of termination of main programme
        :param: mock_flush
        :param: mock_sleep
        :return:
        """
        self.assertTrue(self.metric_builder._flush_thread.is_alive())
        self.metric_builder.shutdown()
        self.assertFalse(self.metric_builder._flush_thread.is_alive())
        self.assertTrue(self.metric_builder._shutdown_event.is_set())
        self.logger.info.assert_any_call("Thread Shutdown Initiated")
        self.logger.info.assert_any_call("Flush Thread Stopped")


class TestMetricBuilderFlushMetrics(unittest.TestCase):
    """
    This Class Validates functionality for
        flush_metrics_to_splunk
    """

    def setUp(self):
        """
        Setup Initial Parameters
        :return:
        """
        self.static_vals = {
            "host": "test-host",
            "env": "test-env",
            "region": "test-region"
        }
        self.logger = MagicMock()

    @patch("time.sleep")
    @patch("src.main.observability.splunk.http_event_collector.builder.metric_builder.threading.Thread.start")
    @patch("src.main.observability.splunk.http_event_collector.builder.metric_builder.splunk_metric_retry_decorator")
    def test_flush_metrics_to_splunk_empty_buffer(self, mock_splunk_metric_retry_decorator, mock_thread, mock_sleep):
        """
        Test Flush Metrics to Splunk and adjust the Buffers
        Patching The Threading Module to Test
        :param: mock_splunk_metric_retry_decorator
        :param: mock_thread
        :param: mock_sleep
        :return:
        """
        mock_sleep.return_value = None
        mock_thread.return_value = None
        mock_splunk_metric_retry_decorator.return_value = None
        metric_builder = MetricBuilder(
            static_vals=self.static_vals,
            flush_interval_seconds=0,
            max_batch_size=2,
            logger=self.logger
        )
        metric_builder.flush_metrics_to_splunk()
        mock_splunk_metric_retry_decorator.assert_not_called()

    @patch("time.sleep")
    @patch("src.main.observability.splunk.http_event_collector.builder.metric_builder.threading.Thread.start")
    @patch("src.main.observability.splunk.http_event_collector.builder.metric_builder.splunk_metric_retry_decorator")
    @patch("src.main.observability.splunk.http_event_collector.builder.metric_builder.METRIC_BATCH_SIZE", new=-1)
    def test_flush_metrics_to_splunk_non_empty_buffer_items_less_than_batch_size(self, mock_splunk_metric_retry_decorator, mock_thread, mock_sleep):
        """
        Test Flush Metrics to Splunk and adjust the Buffers for Total Length Less than batch_size
        Patching The Threading Module to Test
        :param: mock_splunk_metric_retry_decorator
        :param: mock_thread
        :param: mock_sleep
        :return:
        """
        mock_sleep.return_value = None
        mock_thread.return_value = None
        mock_splunk_metric_retry_decorator.return_value = None
        metric_builder = MetricBuilder(
            static_vals=self.static_vals,
            flush_interval_seconds=0,
            max_batch_size=4,
            logger=self.logger
        )
        metric_builder.buffer_gauge_metrics.extend(
            [
                {"metric": "gauge-1", "value": 1, "dimensions": {}, "timestamp": 123}
            ] * 2
        )
        metric_builder.buffer_counter_metrics.extend(
            [
                {"metric": "counter-1", "value": 10, "dimensions": {}, "timestamp": 123}
            ] * 2
        )
        metric_builder.flush_metrics_to_splunk()
        mock_splunk_metric_retry_decorator.assert_called_once()
        called_args = mock_splunk_metric_retry_decorator.call_args[1]
        payload = called_args["splunk_payload"]
        self.assertIn("splunk_payload", called_args)

        self.assertIn("gauge", payload)
        self.assertIn("counter", payload)

        self.assertEqual(len(payload["gauge"]), 2)
        self.assertEqual(len(payload["counter"]), 2)

        self.assertEqual(len(metric_builder.buffer_gauge_metrics), 0)
        self.assertEqual(len(metric_builder.buffer_counter_metrics), 0)

    @patch("time.sleep")
    @patch("src.main.observability.splunk.http_event_collector.builder.metric_builder.threading.Thread.start")
    @patch("src.main.observability.splunk.http_event_collector.builder.metric_builder.splunk_metric_retry_decorator")
    @patch("src.main.observability.splunk.http_event_collector.builder.metric_builder.METRIC_BATCH_SIZE", new=-1)
    def test_flush_metrics_to_splunk_non_empty_buffer_items_greater_than_batch_size(self, mock_splunk_metric_retry_decorator, mock_thread, mock_sleep):
        """
        Test Flush Metrics to Splunk and adjust the Buffers for Total Length Greater than batch_size
        Patching The Threading Module to Test
        :param: mock_splunk_metric_retry_decorator
        :param: mock_thread
        :param: mock_sleep
        :return:
        """
        mock_sleep.return_value = None
        mock_thread.return_value = None
        mock_splunk_metric_retry_decorator.return_value = None
        metric_builder = MetricBuilder(
            static_vals=self.static_vals,
            flush_interval_seconds=0,
            max_batch_size=5,
            logger=self.logger
        )
        metric_builder.buffer_gauge_metrics.extend(
            [
                {"metric": "gauge-1", "value": 1, "dimensions": {}, "timestamp": 123}
            ] * 3
        )
        metric_builder.buffer_counter_metrics.extend(
            [
                {"metric": "counter-1", "value": 10, "dimensions": {}, "timestamp": 123}
            ] * 3
        )
        metric_builder.flush_metrics_to_splunk()
        mock_splunk_metric_retry_decorator.assert_called_once()
        called_args = mock_splunk_metric_retry_decorator.call_args[1]
        payload = called_args["splunk_payload"]
        self.assertIn("splunk_payload", called_args)

        self.assertIn("gauge", payload)
        self.assertIn("counter", payload)

        self.assertEqual(len(payload["gauge"]), 2)
        self.assertEqual(len(payload["counter"]), 3)

        self.assertEqual(len(metric_builder.buffer_gauge_metrics), 1)
        self.assertEqual(len(metric_builder.buffer_counter_metrics), 0)

"""
Metric Builder Process
"""

# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=pointless-string-statement

import copy
import threading
import time

from src.main.observability.splunk.enum.splunk_metric_typ import SplunkMetricType
from src.main.observability.splunk.http_event_collector.connector.metric_splunk_connector import (
    splunk_metric_retry_decorator,
)
from src.main.observability.splunk.http_event_collector.utils.constants import (
    METRIC_NODE_HOST,
    METRIC_NODE_METRIC_TYPE,
    METRIC_NODE_METRIC_VALUE,
    METRIC_NODE_METRIC_BATCH_ID,
    METRIC_NODE_METRIC_ENV,
    METRIC_NODE_METRIC_REGION,
    METRIC_NODE_METRIC,
    METRIC_NODE_DIMENSIONS,
    METRIC_NODE_TIMESTAMP,
    METRIC_BATCH_SIZE,
    METRIC_FLUSH_INTERVAL_SECONDS,
    METRIC_CHECK_INTERVAL_SECONDS,
)


class MetricBuilder:
    """
    Metric Builder Class
    """

    def __init__(self, static_vals: dict, flush_interval_seconds: float, max_batch_size: int, logger):
        """
        Initialize Common Attributes
        :param: static_vals
        :param: flush_interval_seconds
        :param: max_batch_size
        :param: logger
        """
        """
        Class Parameters initialize
        """
        self.static_vals: dict = static_vals
        self.inp_flush_interval_seconds: float = flush_interval_seconds
        self.inp_max_batch_size: int = max_batch_size
        self.logger = logger
        """
        Create a Threading Lock and Buffers for Both Gauge & Counter Metrics
        Adding RLock for nested re-entrant locks
        """
        self.lock = threading.RLock()
        self.buffer_gauge_metrics = []
        self.buffer_counter_metrics = []
        """
        Set the Flush Interval, Batch Size and Check Interval
        """
        self.flush_interval: float = max(self.inp_flush_interval_seconds, METRIC_FLUSH_INTERVAL_SECONDS)
        self.max_batch_size: int = max(self.inp_max_batch_size, METRIC_BATCH_SIZE)
        self._check_interval: float = METRIC_CHECK_INTERVAL_SECONDS
        """
        Initiate the common framework for Metrics
        """
        self.metric_struct = self.__construct_metric()
        """
        Set method __periodic_flush for periodic flush in a separate thread to run asynchronously
        """
        self._shutdown_event = threading.Event()
        self._flush_thread = threading.Thread(target=self.__periodic_flush, daemon=True)
        self._flush_thread.start()

    def __construct_metric(self):
        """
        Construct the Metric Skeleton for Gauge & Counter
        :return:
        """
        metric_framework: dict = {
            METRIC_NODE_METRIC: "",
            METRIC_NODE_METRIC_VALUE: -1,
            METRIC_NODE_DIMENSIONS: {
                METRIC_NODE_HOST: self.static_vals[METRIC_NODE_HOST],
                METRIC_NODE_METRIC_ENV: self.static_vals[METRIC_NODE_METRIC_ENV],
                METRIC_NODE_METRIC_REGION: self.static_vals[METRIC_NODE_METRIC_REGION],
                METRIC_NODE_METRIC_BATCH_ID: -1
            },
            METRIC_NODE_TIMESTAMP: -1
        }

        return metric_framework

    def build_metrics(self, batch_metrics_list):
        """
        Fill the Metrics values with runtime values per microbatch
        :param batch_metrics_list:
        :return:
        """
        with self.lock:
            """
            Lock Both the buffers to add elements
            Splunk Plotting is at millisecond level
            """
            epoch_time = int(time.time())*1000
            for each_metric in batch_metrics_list:
                loop_metric = copy.deepcopy(self.metric_struct)
                """
                Fill the runtime values for each microbatch
                """
                match each_metric[METRIC_NODE_METRIC_TYPE]:

                    case SplunkMetricType.GAUGE.value:
                        """
                        Fill Up Gauge Metrics
                        """
                        loop_metric[METRIC_NODE_METRIC] = each_metric[METRIC_NODE_METRIC]
                        loop_metric[METRIC_NODE_METRIC_VALUE] = each_metric[METRIC_NODE_METRIC_VALUE]
                        loop_metric[METRIC_NODE_DIMENSIONS][METRIC_NODE_METRIC_BATCH_ID] = each_metric[
                            METRIC_NODE_METRIC_BATCH_ID]
                        loop_metric[METRIC_NODE_TIMESTAMP] = epoch_time

                        self.buffer_gauge_metrics.append(loop_metric)

                    case SplunkMetricType.COUNTER.value:
                        """
                        Fill Up Counter Metrics
                        """
                        loop_metric[METRIC_NODE_METRIC] = each_metric[METRIC_NODE_METRIC]
                        loop_metric[METRIC_NODE_METRIC_VALUE] = each_metric[METRIC_NODE_METRIC_VALUE]
                        loop_metric[METRIC_NODE_DIMENSIONS][METRIC_NODE_METRIC_BATCH_ID] = each_metric[
                            METRIC_NODE_METRIC_BATCH_ID]
                        loop_metric[METRIC_NODE_TIMESTAMP] = epoch_time

                        self.buffer_counter_metrics.append(loop_metric)

    def flush_metrics_to_splunk(self):
        """
        Flush Metrics to Splunk Observability Cloud & Adjust the Buffers
        :return:
        """
        with self.lock:
            """
            Calculate Total Metrics Size both all buffers
            """
            total_metrics_size = self.__get_current_buffer_size()
            if total_metrics_size == 0:
                self.logger.info("No Metrics are available for Flush to Splunk")
                return

            self.logger.info(f"""Number Of Metrics Available For Flush to Splunk - {str(total_metrics_size)}""")

            if total_metrics_size <= self.max_batch_size:
                """
                If No Of available metrics count is less than size
                """
                gauge_to_send = len(self.buffer_gauge_metrics)
                counter_to_send = len(self.buffer_counter_metrics)
            else:
                """
                If No Of available metrics count is greater than size then slice both the buffers
                """
                gauge_len = len(self.buffer_gauge_metrics)
                gauge_to_send = int(self.max_batch_size * (gauge_len / total_metrics_size))
                counter_to_send = self.max_batch_size - gauge_to_send

            splunk_payload: dict = {}

            if gauge_to_send > 0:
                """
                Generate Gauge Metric Payload
                """
                splunk_payload[SplunkMetricType.GAUGE.value] = self.buffer_gauge_metrics[:gauge_to_send]

            if counter_to_send > 0:
                """
                Generate Counter Metric Payload
                """
                splunk_payload[SplunkMetricType.COUNTER.value] = self.buffer_counter_metrics[:counter_to_send]

            self.logger.info(f"""Number Of Gauge Metrics Available For Push {str(gauge_to_send)}""")
            self.logger.info(f"""Number Of Counter Metrics Available For Push {str(counter_to_send)}""")

            splunk_metric_retry_decorator(
                splunk_payload=splunk_payload,
                logger=self.logger
            )
            """
            Remove Pushed Metrics From Buffer
            """
            self.buffer_gauge_metrics = self.buffer_gauge_metrics[gauge_to_send:]
            self.buffer_counter_metrics = self.buffer_counter_metrics[counter_to_send:]

    def __get_current_buffer_size(self):
        """
        Get Current Size for both Gauge & Counter Buffer
        :return:
        """
        with self.lock:
            combined_len = len(self.buffer_counter_metrics) + len(self.buffer_gauge_metrics)
            return combined_len

    def __periodic_flush(self):
        """
        Periodic Flush / Write to Splunk Based on Time or Batch Size
        :return:
        """
        last_flush_time = time.time()
        self.logger.info(f"""Periodic Flush Started at {str(last_flush_time)}""")
        """
        Keep running while the shutdown event is NOT set.
        """
        while not self._shutdown_event.is_set():
            """
            Sleep for every Check Interval Before Checking the Flush Conditions
            """
            time.sleep(self._check_interval)
            """
            Validate Both Size & Time Interval Flush Conditions and Flush if either
            is breached
            """
            current_buffer_size: int = self.__get_current_buffer_size()
            current_time: float = time.time()
            calculated_interval: float = (current_time - last_flush_time)
            self.logger.info(
                f"""Current Buffer Size {str(current_buffer_size)} and Interval {str(calculated_interval)}"""
            )
            if current_buffer_size >= self.max_batch_size or calculated_interval >= self.flush_interval:
                """
                Flush the Buffer and Reset the time
                """
                self.flush_metrics_to_splunk()
                last_flush_time = time.time()
        """
        One Final Flush after thread shutdown
        """
        self.flush_metrics_to_splunk()

    def shutdown(self):
        """
        Shutdown Thread Gracefully in case of termination of main programme
        :return:
        """
        self.logger.info("Thread Shutdown Initiated")
        self._shutdown_event.set()
        self._flush_thread.join()
        self.logger.info("Flush Thread Stopped")

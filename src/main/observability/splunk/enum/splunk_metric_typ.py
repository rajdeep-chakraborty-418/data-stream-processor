"""
List of Splunk Metric Types
"""

from enum import Enum


class SplunkMetricType(Enum):
    """
    Enum for Splunk Metric Types
    """
    HISTOGRAM = "histogram"
    GAUGE = "gauge"
    COUNTER = "counter"

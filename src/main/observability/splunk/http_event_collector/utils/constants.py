"""
All Configs related to HTTP Event Collector for Splunk
"""
METRIC_NODE_TIME: str = "time"
METRIC_NODE_TIMESTAMP: str = "timestamp"
METRIC_NODE_HOST: str = "host"
METRIC_NODE_EVENT: str = "event"
METRIC_NODE_SOURCE: str = "source"
METRIC_NODE_SOURCE_TYPE: str = "sourcetype"
METRIC_NODE_FIELDS: str = "fields"
METRIC_NODE_METRIC: str = "metric"
METRIC_NODE_DIMENSIONS: str = "dimensions"
METRIC_NODE_METRIC_NAME: str = "metric"
METRIC_NODE_METRIC_NAMESPACE: str = "_metric_namespace"
METRIC_NODE_METRIC_TYPE: str = "metric_type"
METRIC_NODE_METRIC_VALUE: str = "value"
METRIC_NODE_METRIC_BATCH_ID: str = "batch_id"
METRIC_NODE_METRIC_UNIT: str = "_unit"
METRIC_NODE_METRIC_DESCRIPTION: str = "_description"
METRIC_NODE_METRIC_ENV: str = "env"
METRIC_NODE_METRIC_REGION: str = "region"

METRIC_NODE_METRIC_UNIT_GAUGE_VAL: str = "records"
METRIC_NODE_METRIC_UNIT_HISTOGRAM_VAL: str = "ms"
METRIC_NODE_SOURCE_TYPE_VAL: str = "metric"
METRIC_NODE_EVENT_VAL: str = "metric"
METRIC_NODE_SOURCE_VAL: str = "databricks"

"""
Default Flush Interval and Batch Size
"""
METRIC_FLUSH_INTERVAL_SECONDS: float = 30.00
METRIC_BATCH_SIZE: int = 100
METRIC_CHECK_INTERVAL_SECONDS: float = 2.00

import re

from pyspark.sql.functions import (
    udf,
    trim,
    col,
    to_timestamp,
)
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
)

ip_regex = r'^(\d{1,3}\.){3}\d{1,3}$'


def is_valid_ip(ip):
    """
    Validate if the given IP address is a valid IPv4 address.
    :param ip:
    :return:
    """
    if ip is None:
        return False
    if not re.match(ip_regex, ip):
        return False
    parts = ip.split('.')
    return all(0 <= int(p) <= 255 for p in parts)


ip_valid_udf = udf(is_valid_ip, BooleanType())


def transform(spark):
    """
    Transform function for cleansing and normalizing the input DataFrame.
    This function is expected to be called by the ScriptExecutor with a valid Spark session.
    It assumes a table/view named 'input_dataframe' already exists.
    """
    """
    Retrieve the input DataFrame from the Spark session
    """
    input_dataframe = raw_input
    """
    Apply cleansing and normalization
    """
    input_dataframe = input_dataframe.select(
        [trim(col(c)).alias(c) if c != 'bytes' else col(c) for c in input_dataframe.columns]
    )
    input_dataframe = input_dataframe.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssX")
    ).withColumn(
        "bytes_int",
        col("bytes").cast(IntegerType())
    ).withColumn(
        "valid_src_ip",
        ip_valid_udf(col("src_ip"))
    ).withColumn(
        "valid_dest_ip",
        ip_valid_udf(col("dest_ip"))
    ).withColumn(
        "valid_action",
        col("action").isin("ALLOW", "DENY")
    ).withColumn(
        "valid_protocol",
        col("protocol").isin("TCP", "UDP", "ICMP")
    ).withColumn(
        "valid_timestamp",
        col("timestamp").isNotNull()
    ).withColumn(
        "valid_bytes",
        col("bytes_int").isNotNull()
    )

    input_dataframe = input_dataframe.withColumn(
        "valid_record",
        col("valid_src_ip") &
        col("valid_dest_ip") &
        col("valid_action") &
        col("valid_protocol") &
        col("valid_timestamp") &
        col("valid_bytes")
    )

    return input_dataframe

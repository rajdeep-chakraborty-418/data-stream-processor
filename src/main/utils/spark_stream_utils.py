"""
Streaming Utility Methods
"""

# pylint: disable=line-too-long

from pyspark.sql import DataFrame


def spark_read_file_system_stream(spark, file_format: str, options: dict, schema, path: str) -> DataFrame:
    """
    Read Spark Streaming dataframe from File System with JSON Configs provided
    :param spark:
    :param path:
    :param file_format:
    :param options:
    :param schema:
    :return:
    """
    return (
        spark.readStream
        .format(file_format)
        .schema(schema)
        .options(**options)
        .load(path)
    )


def spark_read_kafka_stream(spark, options: dict) -> DataFrame:
    """
    Read Spark Streaming dataframe from Kafka with JSON Configs provided
    :param spark:
    :param options:
    :return:
    """
    input_df = (
        spark.readStream
        .format("kafka")
        .options(**options)
        .load()
    )
    input_df = input_df.selectExpr(
        "CAST(key AS STRING) as event_key",
        "value as event_data"
    )

    return input_df

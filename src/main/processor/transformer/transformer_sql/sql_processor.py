"""
Transformer SQL Rule Type Processor Module
"""

# pylint: disable=pointless-string-statement
# pylint: disable=import-error
# pylint: disable=line-too-long

from pyspark.sql import (
    DataFrame,
    SparkSession,
)

from src.main.utils.constants import (
    NODE_TRANSFORM_CONVERSION_TRANSFORM_NAME,
    NODE_TRANSFORM_CONVERSION_TRANSFORM_QUERY,
    NODE_TRANSFORM_CONVERSION_TRANSFORM_OUTPUT,
    NODE_TRANSFORM_CONVERSION_TRANSFORM_INPUT,
    NODE_TRANSFORM_CONVERSION_TRANSFORM_OUTPUT_TABLE,
)
from src.main.utils.spark_utils import (
    execute_spark_sql,
)


def sql_transformation(spark: SparkSession, input_rule_dict: dict, logger) -> DataFrame:
    """
    Transforms the input DataFrame using SQL transformation rules defined in the input_rule_dict.
    :param spark:
    :param input_rule_dict:
    :param logger
    :return:
    """
    transform_name = input_rule_dict.get(NODE_TRANSFORM_CONVERSION_TRANSFORM_NAME)
    transform_query = input_rule_dict.get(NODE_TRANSFORM_CONVERSION_TRANSFORM_QUERY)
    transform_input = input_rule_dict.get(NODE_TRANSFORM_CONVERSION_TRANSFORM_INPUT)
    transform_output = input_rule_dict.get(NODE_TRANSFORM_CONVERSION_TRANSFORM_OUTPUT)
    """
    Execute the SQL query on the Spark session
    """
    logger.info(f"""Transformer SQL Module invoked with Rule {transform_name}""")
    custom_format = {**transform_input}
    for key, value in custom_format.items():
        transform_query = transform_query.replace("{" + key + "}", value)
    sql_query_dataframe: DataFrame = execute_spark_sql(
        spark=spark,
        sql_query=transform_query
    )
    sql_query_dataframe.createOrReplaceTempView(transform_output[NODE_TRANSFORM_CONVERSION_TRANSFORM_OUTPUT_TABLE])
    logger.info(f"""Transformer SQL Module completed with Rule {transform_name}""")

    return sql_query_dataframe

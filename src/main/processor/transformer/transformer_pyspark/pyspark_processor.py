"""
Transformer Pyspark Rule Type Processor Module
"""
import builtins

# pylint: disable=pointless-string-statement
# pylint: disable=import-error
# pylint: disable=line-too-long

from pyspark.sql import (
    DataFrame,
    SparkSession,
)

from src.main.interface.interface_pyspark_transformer import (
    InterfacePysparkScriptExecutor,
)
from src.main.utils.constants import (
    NODE_TRANSFORM_CONVERSION_TRANSFORM_NAME,
    NODE_TRANSFORM_CONVERSION_TRANSFORM_QUERY,
    NODE_TRANSFORM_CONVERSION_TRANSFORM_OUTPUT,
    NODE_TRANSFORM_CONVERSION_TRANSFORM_INPUT,
    NODE_TRANSFORM_CONVERSION_TRANSFORM_OUTPUT_TABLE,
)


def prepare_pyspark_namespace(spark: SparkSession, input_variables: dict):
    """
    Execute the full PySpark script text dynamically, passing variables like `spark`.
    Each script runs in an isolated namespace that includes builtins plus passed variables
    :param: input_variables
    """
    exec_namespace = {
        '__builtins__': builtins,
    }
    default_variables = {
        'spark': spark
    }
    final_variables = {**default_variables, **input_variables}
    exec_namespace.update(final_variables)
    return exec_namespace


def pyspark_transformation(spark: SparkSession, input_rule_dict: dict, logger) -> DataFrame:
    """
    Transforms the input DataFrame using Pyspark transformation rules defined in the input_rule_dict.
    :param spark:
    :param input_rule_dict:
    :param logger
    :return:
    """
    transform_name = input_rule_dict.get(NODE_TRANSFORM_CONVERSION_TRANSFORM_NAME)
    transform_query = input_rule_dict.get(NODE_TRANSFORM_CONVERSION_TRANSFORM_QUERY)
    transform_input = input_rule_dict.get(NODE_TRANSFORM_CONVERSION_TRANSFORM_INPUT)
    transform_output = input_rule_dict.get(NODE_TRANSFORM_CONVERSION_TRANSFORM_OUTPUT)
    logger.info(f"""Transformer Pyspark Module invoked with Rule {transform_name}""")
    """
    Convert List of input table names in a list of string
    """
    list_tables: list[str] = list(transform_input.values())
    """
    Invoke the Pyspark Interface Script Executor For Script Validation
    and Transformer Method Execution
    """
    executor = InterfacePysparkScriptExecutor(spark, transform_query, list_tables)
    output_dataframe = executor.run()
    output_dataframe.createOrReplaceTempView(transform_output[NODE_TRANSFORM_CONVERSION_TRANSFORM_OUTPUT_TABLE])
    logger.info(f"""Transformer Pyspark Module completed with Rule {transform_name}""")
    return output_dataframe

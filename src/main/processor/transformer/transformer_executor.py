"""
Transformer Executor Module
"""

# pylint: disable=pointless-string-statement
# pylint: disable=import-error
# pylint: disable=line-too-long


from typing import Any

from pyspark.sql import (
    DataFrame,
    SparkSession,
)

from src.main.enum.config_file_type import ConfigFileType
from src.main.enum.transformer_type import (
    TransformerType,
)
from src.main.processor.transformer.transformer_pyspark.pyspark_processor import (
    pyspark_transformation,
)
from src.main.processor.transformer.transformer_sql.sql_processor import (
    sql_transformation,
)
from src.main.utils.constants import (
    NODE_TRANSFORM_CONVERSION,
    NODE_TRANSFORM_INPUT,
    NODE_TRANSFORM_CONVERSION_TRANSFORM_TYP,
    NODE_TRANSFORM_CONVERSION_TRANSFORM_SEQ,
    NODE_TRANSFORM_CONVERSION_TRANSFORM_QUERY,
    NODE_AWS_REGION,
    NODE_CONFIG_READER_SOURCE,
)
from src.main.utils.processor_utils import (
    read_config,
)


def sort_transformation_rule_config(input_config: list[dict], sort_attribute: str) -> Any:
    """
    Sorts the transformation rule configuration based on the order of execution.
    :param input_config:
    :param sort_attribute:
    :return:
    """
    return sorted(input_config, key=lambda x: int(x[sort_attribute]))


def transformer_executor(spark: SparkSession, batch_df: DataFrame, config: dict, logger) -> DataFrame:
    """
    Call the Appropriate Child Transformer Type Method and get the batch dataframe
    :param spark:
    :param batch_df:
    :param config:
    :param logger:
    :return:
    """
    """
    Memorise the Batch DF to In Memory View
    """
    batch_df.createOrReplaceTempView(config[NODE_TRANSFORM_INPUT])
    """
    Iterate each transformation rule and apply the transformation logic
    """
    output_dataframe: DataFrame = None
    for item in config[NODE_TRANSFORM_CONVERSION]:
        """
        Invoke transformation types with rules
        """
        match item[NODE_TRANSFORM_CONVERSION_TRANSFORM_TYP]:

            case TransformerType.SQL.value:

                output_dataframe = sql_transformation(
                    spark=spark,
                    input_rule_dict=item,
                    logger=logger
                )

            case TransformerType.PYSPARK.value:

                output_dataframe = pyspark_transformation(
                    spark=spark,
                    input_rule_dict=item,
                    logger=logger
                )

        logger.info(f"""No Of Transformed Records {output_dataframe.count()}""")

    return output_dataframe


def transformer_process_configs(transformer_config: dict, logger) -> dict:
    """
    Processes the Transformer configurations to prepare them for execution.
    :param transformer_config:
    :param logger:
    :return:
    """
    """
    Sort the transformation conversion rules based on the 'transform_seq' attribute.
    """
    sorted_transformation_rule_config = sort_transformation_rule_config(
        input_config=transformer_config[NODE_TRANSFORM_CONVERSION],
        sort_attribute=NODE_TRANSFORM_CONVERSION_TRANSFORM_SEQ
    )
    """
    Update the Sorted Rules Inplace
    """
    transformer_config[NODE_TRANSFORM_CONVERSION] = sorted_transformation_rule_config
    """
    For Each Rule Load the Query from File
    """
    for item in sorted_transformation_rule_config:
        """
        Get transformation File rules into string rules
        """
        loop_transform_query = None
        match item[NODE_TRANSFORM_CONVERSION_TRANSFORM_TYP]:

            case TransformerType.SQL.value:
                """
                Load the SQL Query from File
                """
                loop_transform_query = read_config(
                    input_region=transformer_config[NODE_AWS_REGION],
                    input_file_path=item[NODE_TRANSFORM_CONVERSION_TRANSFORM_QUERY],
                    input_file_typ=ConfigFileType.SQL.value,
                    input_config_reader_source=transformer_config[NODE_CONFIG_READER_SOURCE]
                )

            case TransformerType.PYSPARK.value:
                """
                Load the PySpark Query from File
                """
                loop_transform_query = read_config(
                    input_region=transformer_config[NODE_AWS_REGION],
                    input_file_path=item[NODE_TRANSFORM_CONVERSION_TRANSFORM_QUERY],
                    input_file_typ=ConfigFileType.PYSPARK.value,
                    input_config_reader_source=transformer_config[NODE_CONFIG_READER_SOURCE]
                )

        """
        Replace File Path with Content for the respective Rule
        """
        item[NODE_TRANSFORM_CONVERSION_TRANSFORM_QUERY] = loop_transform_query

    return transformer_config

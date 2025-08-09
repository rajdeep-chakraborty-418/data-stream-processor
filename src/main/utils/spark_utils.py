"""
Spark Utility Methods
"""
# pylint: disable=pointless-string-statement
# pylint: disable=too-many-arguments

from pyspark.sql import (
    SparkSession,
    DataFrame,
)
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    BooleanType,
    ArrayType,
    StructType,
    StructField,
)


def generate_spark_schema_from_json(input_json_dict: dict):
    """
    Generate a struct like pyspark compatible schema from JSON schema
    :param input_json_dict:
    :return:
    """
    return StructType.fromJson(input_json_dict)


def generate_spark_schema_from_avro(input_json_dict: dict):
    """
    Generate a struct like pyspark compatible schema from AVRO schema
    :param input_json_dict:
    :return:
    """

    def convert_avro_datatype_spark_datatype(input_field):
        """
        Convert Avro Datatype to Spark Datatype
        :param input_field:
        :return:
        """
        avro_type = input_field["type"]
        """ 
        If the field is a union, choose the non-null type
        """
        if isinstance(avro_type, list):
            avro_type = [t for t in avro_type if t != "null"][0]
        """
        Convert Nuclear data types
        """
        match avro_type:
            case "string":
                return StringType()
            case "int":
                return IntegerType()
            case "long":
                return LongType()
            case "float":
                return FloatType()
            case "double":
                return DoubleType()
            case "boolean":
                return BooleanType()
        """
        Convert Complex / Nested data types
        """
        if isinstance(avro_type, dict) and avro_type.get("type") == "array":
            return ArrayType(
                convert_avro_datatype_spark_datatype(
                    {
                        "type": avro_type["items"]
                    }
                )
            )
        elif isinstance(avro_type, dict) and avro_type.get("type") == "record":
            return StructType(
                [
                    StructField(f["name"], convert_avro_datatype_spark_datatype(f), True)
                    for f in avro_type["fields"]
                ]
            )

    """
    Process top level fields
    """
    struct_fields = [
        StructField(
            field["name"], convert_avro_datatype_spark_datatype(field), True
        )
        for field in input_json_dict["fields"]
    ]
    return StructType(struct_fields)


def create_spark_session(app_name: str, spark_configs: dict = None) -> SparkSession:
    """
    Create a Spark Session
    :param spark_configs:
    :param app_name:
    :return:
    """
    builder = (SparkSession
               .builder
               .appName(app_name)
               )

    if spark_configs is not None:

        for key, value in spark_configs.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()


def execute_spark_sql(spark: SparkSession, sql_query: str) -> DataFrame:
    """
    Execute a SQL query on the Spark session.
    :param spark: The Spark session.
    :param sql_query: The SQL query to execute.
    :return: None
    """
    return spark.sql(sql_query)


def spark_batch_write_file_system(
        input_dataframe,
        output_mode: str,
        partition_columns: list[str],
        options: dict,
        file_format: str,
        output_path: str) -> None:
    """
    Write a Spark DataFrame to the file system in the specified format.
    :param input_dataframe:
    :param output_mode:
    :param partition_columns:
    :param options:
    :param file_format:
    :param output_path:
    :return:
    """
    input_dataframe.write \
        .mode(output_mode) \
        .partitionBy(*partition_columns) \
        .options(**options) \
        .format(file_format) \
        .save(output_path)


def format_kafka_json_dataframe(input_dataframe: DataFrame, input_schema) -> DataFrame:
    """
    Format a Kafka JSON DataFrame to match the input schema.
    :param input_dataframe:
    :param input_schema:
    :return:
    """
    input_dataframe = input_dataframe.withColumn(
        "data", from_json(
            col("event_data").cast(
                StringType()
            ), input_schema
        )
    ).select(
        "event_key",
        "event_data",
        "data.*"
    )
    return input_dataframe


def format_kafka_avro_dataframe(input_dataframe: DataFrame, input_schema) -> DataFrame:
    """
    Format a Kafka AVRO DataFrame to match the input schema.
    :param input_dataframe:
    :param input_schema:
    :return:
    """
    input_dataframe = input_dataframe.withColumn(
        "data", from_avro(
            col("event_data"), input_schema
        )
    ).select(
        "event_key",
        "event_data",
        "data.*"
    )
    return input_dataframe


def spark_batch_write_kafka(input_dataframe, options: dict) -> None:
    """
    Write a Spark DataFrame to the Kafka in pre transformed key, value format.
    :param input_dataframe:
    :param options:
    :return:
    """
    input_dataframe.write \
        .format("kafka") \
        .options(**options) \
        .save()

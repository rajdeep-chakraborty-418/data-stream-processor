"""
Test Classes for Reusable Spark Methods
"""

# pylint: disable=import-error
# pylint: disable=line-too-long

import unittest
from unittest.mock import (
    MagicMock,
    patch,
)

from pyspark.sql import (
    SparkSession,
    DataFrame,
)
from pyspark.sql.functions import col

from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
    BooleanType,
    DoubleType,
    FloatType,
    StructField,
    BinaryType,
)

from src.main.utils.spark_utils import (
    generate_spark_schema_from_json,
    create_spark_session,
    generate_spark_schema_from_avro,
    execute_spark_sql,
    spark_batch_write_file_system,
    format_kafka_json_dataframe,
    format_kafka_avro_dataframe,
    spark_batch_write_kafka,
)


class TestSparkUtilsGenerateSparkSchemaFromJson(unittest.TestCase):
    """
    Test Class for generate_spark_schema_from_json
    """

    def setUp(self):
        """
        Setup Test Json data
        :return:
        """
        self.json_schema = {
            "type": "struct",
            "fields": [
                {
                    "name": "id",
                    "type": "integer",
                    "nullable": True,
                    "metadata": {}
                },
                {
                    "name": "user",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "name",
                                "type": "string",
                                "nullable": True,
                                "metadata": {}
                            },
                            {
                                "name": "email",
                                "type": "string",
                                "nullable": True,
                                "metadata": {}
                            }
                        ]
                    },
                    "nullable": True,
                    "metadata": {}
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "nullable": True,
                    "metadata": {}
                }
            ]
        }

    def test_generate_spark_schema_from_json(self):
        """
        Test Generate a struct like pyspark compatible schema
        :return:
        """
        schema = generate_spark_schema_from_json(self.json_schema)
        self.assertIsInstance(schema, StructType)
        self.assertEqual(len(schema.fields), 3)
        self.assertEqual(schema["id"].dataType, IntegerType())
        self.assertEqual(schema["created_at"].dataType, TimestampType())
        self.assertEqual(schema["user"].dataType.typeName(), "struct")
        self.assertEqual(len(schema["user"].dataType.fields), 2)
        self.assertEqual(schema["user"].dataType["name"].dataType, StringType())
        self.assertEqual(schema["user"].dataType["email"].dataType, StringType())


class TestSparkUtilsCreateSparkSession(unittest.TestCase):
    """
    Test Spark Creation Utilities
    """

    def setUp(self):
        """
        Setup Test Config data
        :return:
        """
        self.app_name = 'SparkUnitTest'
        self.custom_configs = {
            "spark.executor.memory": "1g",
            "spark.sql.shuffle.partitions": "5"
        }
        self.spark_default = create_spark_session(self.app_name)
        self.spark_custom = create_spark_session(self.app_name, self.custom_configs)

    def test_create_default_session(self):
        """
        Test if Spark is created without Config
        :return:
        """
        self.assertIsInstance(self.spark_default, SparkSession)
        self.assertEqual(self.spark_default.sparkContext.appName, self.app_name)

    def test_create_with_custom_configs(self):
        """
        Test if Spark is created with Config
        :return:
        """
        self.assertEqual(self.spark_custom.conf.get("spark.executor.memory"), "1g")
        self.assertEqual(self.spark_custom.conf.get("spark.sql.shuffle.partitions"), "5")

    def tearDown(self):
        """
        Tear Down spark instance
        :return:
        """
        self.spark_default.stop()
        self.spark_custom.stop()


class TestSparkUtilsGenerateSparkSchemaFromAvro(unittest.TestCase):
    """
    Test Class for generate_spark_schema_from_avro
    """

    def setUp(self):
        """
        Setup Test AVRO Schema data
        :return:
        """
        self.avro_schema = {
            "type": "record",
            "name": "SXPBinding",
            "namespace": "com.cisco.sbg.objectservice.dynamicobjects.sxpb",
            "fields": [
                {
                    "name": "id",
                    "type": "int"
                },
                {
                    "name": "iseDeploymentId",
                    "type": "double"
                },
                {
                    "name": "eventType",
                    "type": "string"
                },
                {
                    "name": "operation",
                    "type": "boolean"
                },
                {
                    "name": "bindings",
                    "type": {
                        "type": "record",
                        "name": "Binding",
                        "fields": [
                            {"name": "ip_prefix", "type": "long"},
                            {"name": "ip_prefix_length", "type": "long"},
                            {"name": "tag", "type": "long"},
                            {"name": "source_address", "type": "float"},
                            {"name": "peer_sequence", "type": {"type": "array", "items": "long"}},
                            {"name": "vpn", "type": "string"}
                        ]
                    }
                },
                {
                    "name": "orgId",
                    "type": "long"
                },
                {
                    "name": "metadata",
                    "type": {
                        "type": "record",
                        "name": "MetadataSXP",
                        "fields": [
                            {"name": "timestamp", "type": "string"},
                            {"name": "version", "type": "string"}
                        ]
                    }
                }
            ]
        }

    def test_generate_spark_schema_from_avro(self):
        """
        Test Generate a struct like pyspark compatible schema from AVRO schema
        :return:
        """
        schema = generate_spark_schema_from_avro(self.avro_schema)
        self.assertIsInstance(schema, StructType)
        self.assertEqual(len(schema.fields), 7)
        self.assertEqual(schema["id"].dataType, IntegerType())
        self.assertEqual(schema["iseDeploymentId"].dataType, DoubleType())
        self.assertEqual(schema["eventType"].dataType, StringType())
        self.assertEqual(schema["operation"].dataType, BooleanType())
        self.assertEqual(schema["bindings"].dataType.typeName(), "struct")
        self.assertEqual(len(schema["bindings"].dataType.fields), 6)
        self.assertEqual(schema["bindings"].dataType["ip_prefix"].dataType, LongType())
        self.assertEqual(schema["bindings"].dataType["ip_prefix_length"].dataType, LongType())
        self.assertEqual(schema["bindings"].dataType["tag"].dataType, LongType())
        self.assertEqual(schema["bindings"].dataType["source_address"].dataType, FloatType())
        self.assertEqual(schema["bindings"].dataType["peer_sequence"].dataType.typeName(), "array")
        self.assertEqual(schema["bindings"].dataType["peer_sequence"].dataType.elementType, LongType())
        self.assertEqual(schema["bindings"].dataType["vpn"].dataType, StringType())
        self.assertEqual(schema["orgId"].dataType, LongType())
        self.assertEqual(schema["metadata"].dataType.typeName(), "struct")
        self.assertEqual(len(schema["metadata"].dataType.fields), 2)
        self.assertEqual(schema["metadata"].dataType["timestamp"].dataType, StringType())
        self.assertEqual(schema["metadata"].dataType["version"].dataType, StringType())


class TestExecuteSparkSql(unittest.TestCase):
    """
    Test Class for execute_spark_sql function
    """

    def setUp(self):
        """
        Setup Mock Spark Session and DataFrame
        :return:
        """
        self.mock_spark = MagicMock(spec=SparkSession)
        self.mock_df = MagicMock(spec=DataFrame)
        self.mock_spark.sql.return_value = self.mock_df

    def test_execute_spark_sql_success(self):
        """
        Test execute_spark_sql function with a valid SQL query
        :return:
        """
        sql_query = """Valid Query"""
        result = execute_spark_sql(self.mock_spark, sql_query)
        self.mock_spark.sql.assert_called_once_with(sql_query)
        self.assertIsInstance(result, DataFrame)
        self.assertEqual(result, self.mock_df)

    def test_execute_spark_sql_invalid_query(self):
        """
        Test execute_spark_sql function with an invalid SQL query
        :return:
        """
        sql_query = "Invalid Query"
        self.mock_spark.sql.side_effect = Exception("SQL Execution Failed")
        with self.assertRaises(Exception) as context:
            execute_spark_sql(self.mock_spark, sql_query)
        self.mock_spark.sql.assert_called_once_with(sql_query)
        self.assertEqual(str(context.exception), "SQL Execution Failed")


class TestSparkBatchWriteFileSystem(unittest.TestCase):
    """
    Test Class for Spark Batch Write to File System
    """

    def setUp(self):
        """
        Setup Mock Spark Session and DataFrame
        :return:
        """
        self.mock_df = MagicMock()
        self.mock_writer = MagicMock()
        self.mock_mode = MagicMock()
        self.mock_partitionBy = MagicMock()
        self.mock_options = MagicMock()
        self.mock_format = MagicMock()
        self.output_mode = "test_append"
        self.partition_columns = ["col1", "col2"]
        self.options = {"option_1": "true", "option_2": "test_val"}
        self.file_format = "file_format"
        self.output_path = "/fake/output/path"

    def test_spark_batch_write_file_system_with_mock(self):
        """
        Test the spark_batch_write_file_system function with a mock DataFrame
        :return:
        """
        self.mock_df.write = self.mock_writer
        self.mock_writer.mode.return_value = self.mock_mode
        self.mock_mode.partitionBy.return_value = self.mock_partitionBy
        self.mock_partitionBy.options.return_value = self.mock_options
        self.mock_options.format.return_value = self.mock_format
        self.mock_format.save.return_value = None

        spark_batch_write_file_system(
            input_dataframe=self.mock_df,
            output_mode=self.output_mode,
            partition_columns=self.partition_columns,
            options=self.options,
            file_format=self.file_format,
            output_path=self.output_path
        )
        self.mock_writer.mode.assert_called_once_with(self.output_mode)
        self.mock_mode.partitionBy.assert_called_once_with(*self.partition_columns)
        self.mock_partitionBy.options.assert_called_once_with(**self.options)
        self.mock_options.format.assert_called_once_with(self.file_format)
        self.mock_format.save.assert_called_once_with(self.output_path)


class TestFormatKafkaDataframe(unittest.TestCase):
    """
    Test Class for formatting Kafka DataFrame
    """

    def setUp(self):
        """
        Setup Spark Session and DataFrame
        :return:
        """
        self.spark = create_spark_session("TestFormatKafkaDataframe")

    def tearDown(self):
        """
        Tear Down Spark Session
        :return:
        """
        self.spark.stop()

    def test_format_kafka_json_dataframe(self):
        """
        Test formatting a Kafka JSON DataFrame to match the input schema.
        :return:
        """
        input_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("contact", StructType([
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True)
            ]))
        ])
        input_data = [
            ("key-1", '{"id": 1, "name": "Alice", "contact": {"email": "alice@example.com", "phone": "123-456-7890"}}')
        ]
        input_df = self.spark.createDataFrame(input_data, ["event_key", "event_data"])
        result_df = format_kafka_json_dataframe(
            input_df,
            input_schema
        )
        results = result_df.collect()
        self.assertEqual(results[0]["event_key"], "key-1")
        self.assertEqual(results[0]["id"], 1)
        self.assertEqual(results[0]["name"], "Alice")
        self.assertEqual(results[0]["contact"]["email"], "alice@example.com")
        self.assertEqual(results[0]["contact"]["phone"], "123-456-7890")

        expected_schema = StructType([
            StructField("event_key", StringType(), True),
            StructField("event_data", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("contact", StructType([
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True)
            ]), True)
        ])
        self.assertEqual(result_df.schema.simpleString(), expected_schema.simpleString())

    @patch('src.main.utils.spark_utils.from_avro')
    def test_format_kafka_avro_dataframe(self, mock_from_avro):
        """
        Test formatting a Kafka AVRO DataFrame to match the input schema.
        :param mock_from_avro:
        :return:
        """
        input_df = MagicMock(spec=DataFrame)
        mock_data_column = MagicMock(name="Column")
        mock_from_avro.return_value = mock_data_column
        with_column_df = MagicMock(spec=DataFrame)
        input_df.withColumn.return_value = with_column_df
        final_df = MagicMock(spec=DataFrame)
        with_column_df.select.return_value = final_df

        result_df = format_kafka_avro_dataframe(
            input_df,
            "dummy_schema"
        )

        mock_from_avro.assert_called_once()
        called_col_arg, called_schema_arg = mock_from_avro.call_args[0]
        self.assertEqual(
            called_col_arg._jc.toString(),
            col("event_data")._jc.toString()
        )
        self.assertEqual(called_schema_arg, "dummy_schema")

        input_df.withColumn.assert_called_once_with(
            "data",
            mock_data_column
        )
        with_column_df.select.assert_called_once_with(
            "event_key",
            "event_data",
            "data.*"
        )
        self.assertEqual(result_df, final_df)


class TestSparkBatchWriteKafka(unittest.TestCase):
    """
    Test Class for Write a Spark DataFrame to the Kafka
    """

    def setUp(self):
        """
        Setup Mock Spark Session and DataFrame
        :return:
        """
        self.mock_df = MagicMock()
        self.mock_writer = MagicMock()
        self.mock_format = MagicMock()
        self.mock_options = MagicMock()
        self.options = {"option_1": "test-1", "option_2": "test-2"}

    def test_spark_batch_write_kafka(self):
        """
        Test the spark_batch_write_kafka function with a mock DataFrame
        :return:
        """
        self.mock_df.write = self.mock_writer
        self.mock_writer.format.return_value = self.mock_format
        self.mock_format.options.return_value = self.mock_options
        self.mock_options.save.return_value = None

        spark_batch_write_kafka(
            input_dataframe=self.mock_df,
            options=self.options,
        )
        self.mock_writer.format.assert_called_once_with("kafka")
        self.mock_format.options.assert_called_once_with(**self.options)

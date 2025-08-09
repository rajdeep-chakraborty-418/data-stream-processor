"""
Writer Executor Unit Tests
"""

# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=pointless-string-statement


import json
import unittest
from typing import Any
from unittest import mock
from unittest.mock import MagicMock, patch

from pyspark.sql import DataFrame

from src.main.processor.writer.writer_executor import (
    writer_executor,
    writer_process_micro_batch,
    writer_process_configs, formulate_metric_dictionary,
)
from src.main.utils.json_utils import (
    StructDict,
)
from src.main.utils.spark_utils import (
    create_spark_session,
)


class TestWriterExecutor(unittest.TestCase):
    """
    Unit tests for the writer_executor function in the writer module.
    """

    def setUp(self):
        """
        Initialise Test Config
        :return:
        """
        self.mock_input_dataframe = MagicMock(spec=DataFrame)
        self.config = """
        {
            "aws_region": "test_region",
            "environment": "test_env",
            "logger_name": "test_logger",
            "processor_typ": "test_processor",
            "transform": {
                "input": "test_transform_input",
                "conversion": [
                  {
                    "transform_typ": "test_typ",
                    "transform_output": "test_output"
                  }
                ],
                "output": "test_transform_output"
            },
            "output": {
                "writer_typ": "test_writer",
                "targetPath": "test_aws",
                "format": "test_format",
                "outputMode": "test_mode",
                "stream_options": {
                  "option-1": "test-option-1",
                  "option-2": "test-option-2"
                }
            }
        }
        """
        self.mock_splunk_builder = mock.MagicMock()
        self.mock_splunk_config = {
            "metric_builder": self.mock_splunk_builder,
            "metric_config": Any
        }
        self.json_config = json.loads(self.config)
        self.mock_config = StructDict(**self.json_config)
        self.mock_logger = MagicMock()
        self.batch_df_sample_data = [("Alice", 1), ("Bob", 2)]
        self.batch_df_columns = ["name", "value"]
        self.spark = create_spark_session("test_micro_batch")
        self.batch_df = self.spark.createDataFrame(self.batch_df_sample_data, self.batch_df_columns)
        self.mock_formulate_metric_dictionary_output = [
            {
                "metric_type": "gauge",
                "metric": "test-gauge-1",
                "value": -1,
                "batch_id": -1
            },
            {
                "metric_type": "counter",
                "metric": "test-counter-1",
                "value": -1,
                "batch_id": -1
            }
        ]

    @patch('src.main.processor.writer.writer_executor.writer_process_micro_batch')
    def test_writer_executor(self, mock_writer_process_micro_batch):
        """
        Test the writer_executor function to ensure it initializes the write stream correctly and processes
        micro-batches.
        :param mock_writer_process_micro_batch:
        :return:
        """
        mock_writer_config = {
            "writer_typ": "file_system",
            "writer_all_options": {
                "output_mode": "test-mode",
                "partition_by": [],
                "write_options": {},
                "write_format": "test_format",
                "output_path": "test_output_path"
            },
            "stream_options": {
                "option-1": "test-option-1",
                "option-2": "test-option-2"
            }
        }
        mock_transformer_config = {
            "input": "test_transform_input",
            "conversion": [
                {
                    "transform_typ": "test_typ",
                    "transform_output": "test_output"
                }
            ],
            "output": "test_transform_output"
        }

        """
        Chain Mock the write_stream process
        """
        self.mock_input_dataframe.writeStream.foreachBatch.return_value = self.mock_input_dataframe.writeStream
        self.mock_input_dataframe.writeStream.outputMode.return_value = self.mock_input_dataframe.writeStream
        self.mock_input_dataframe.writeStream.options.return_value = self.mock_input_dataframe.writeStream
        self.mock_input_dataframe.writeStream.start.return_value = self.mock_input_dataframe.writeStream
        self.mock_input_dataframe.writeStream.awaitTermination.return_value = None

        writer_executor(
            self.mock_input_dataframe,
            mock_writer_config,
            mock_transformer_config,
            self.mock_logger,
            self.mock_splunk_config
        )
        self.mock_input_dataframe.writeStream.foreachBatch.assert_called_once()
        self.mock_input_dataframe.writeStream.outputMode.assert_called_once_with(
            mock_writer_config['writer_all_options']['output_mode']
        )
        self.mock_input_dataframe.writeStream.options.assert_called_once_with(
            **{
                "option-1": "test-option-1",
                "option-2": "test-option-2",
            }
        )
        self.mock_input_dataframe.writeStream.start.assert_called_once()
        self.mock_input_dataframe.writeStream.awaitTermination.assert_called_once()

        batch_callback = self.mock_input_dataframe.writeStream.foreachBatch.call_args[0][0]
        batch_callback(self.mock_input_dataframe, 123)
        self.assertEqual(len(mock_writer_process_micro_batch.call_args_list[0][0]), 6)
        mock_writer_process_micro_batch.assert_called_once_with(
            self.mock_input_dataframe,
            123,
            mock_writer_config,
            mock_transformer_config,
            self.mock_logger,
            self.mock_splunk_config
        )

    @patch('src.main.processor.writer.writer_executor.transformer_executor')
    @patch('src.main.processor.writer.writer_executor.spark_batch_write_file_system')
    @patch('src.main.processor.writer.writer_executor.formulate_metric_dictionary')
    def test_writer_process_micro_batch_file_system(self, mock_formulate_metric_dictionary, mock_spark_batch_write_file_system, mock_transformer_executor):
        """
        Test Processes a micro-batch of data in the writer stream with transformations or actions as needed.
        :param: mock_transformer_executor
        :param: mock_spark_batch_write_file_system
        :param: mock_metric_executor
        :return:
        """
        mock_output_df = MagicMock()
        mock_formulate_metric_dictionary.return_value = self.mock_formulate_metric_dictionary_output
        mock_transformer_executor.return_value = mock_output_df
        mock_spark_batch_write_file_system.return_value = None
        batch_id = 1
        mock_writer_config = {
            "writer_typ": "file_system",
            "writer_all_options": {
                "output_mode": "test-mode",
                "partition_by": [],
                "write_options": {},
                "write_format": "test_format",
                "target_path": "test_output_path"
            }
        }
        mock_transformer_config = {
            "transformer_key": "value"
        }
        writer_process_micro_batch(
            self.batch_df,
            batch_id,
            mock_writer_config,
            mock_transformer_config,
            self.mock_logger,
            self.mock_splunk_config
        )
        mock_transformer_executor.assert_called_once_with(
            batch_df=self.batch_df,
            spark=self.batch_df.sparkSession,
            config=mock_transformer_config,
            logger=self.mock_logger
        )
        mock_spark_batch_write_file_system.assert_called_once_with(
            input_dataframe=mock_output_df,
            output_mode=mock_writer_config['writer_all_options']['output_mode'],
            partition_columns=mock_writer_config['writer_all_options']['partition_by'],
            options=mock_writer_config['writer_all_options']['write_options'],
            file_format=mock_writer_config['writer_all_options']['write_format'],
            output_path=mock_writer_config['writer_all_options']['target_path']
        )
        self.mock_logger.info.assert_any_call(f"Batch Transformation Initiated for Micro Batch with ID: {batch_id}")
        self.mock_logger.info.assert_any_call(f"Batch Transformation Completed for Micro Batch with ID: {batch_id}")
        self.mock_logger.info.assert_any_call(f"Batch Write Initiated for Micro Batch with ID: {batch_id}")
        self.mock_logger.info.assert_any_call(f"Batch Write Completed for Micro Batch with ID: {batch_id}")

        self.mock_splunk_builder.build_metrics.assert_called_once_with(
            batch_metrics_list=mock.ANY,
        )
        call_args = self.mock_splunk_builder.build_metrics.call_args[1]
        batch_metrics = call_args['batch_metrics_list']
        self.assertEqual(len(batch_metrics), 2)

        counter_count = sum(1 for metric in batch_metrics if metric['metric_type'] == 'counter')
        gauge_count = sum(1 for metric in batch_metrics if metric['metric_type'] == 'gauge')

        self.assertEqual(counter_count, 1)
        self.assertEqual(gauge_count, 1)

        mock_formulate_metric_dictionary.assert_called_once()
        call_args = mock_formulate_metric_dictionary.call_args[1]
        self.assertEqual(call_args['splunk_metric_config'], self.mock_splunk_config['metric_config'])
        self.assertEqual(call_args['batch_id'], 1),
        assert "microbatch_time" in call_args['actual_metric']
        assert "microbatch_transformation_time" in call_args['actual_metric']
        assert "microbatch_writer_time" in call_args['actual_metric']
        assert "microbatch_writer_record_count" in call_args['actual_metric']
        assert "microbatch_record_count" in call_args['actual_metric']


    @patch('src.main.processor.writer.writer_executor.transformer_executor')
    @patch('src.main.processor.writer.writer_executor.spark_batch_write_file_system')
    @patch('src.main.processor.writer.writer_executor.spark_batch_write_kafka')
    @patch('src.main.processor.writer.writer_executor.formulate_metric_dictionary')
    def test_writer_process_micro_batch_message_broker_kafka(self, mock_formulate_metric_dictionary, mock_spark_batch_write_kafka, mock_spark_batch_write_file_system, mock_transformer_executor):
        """
        Test Processes a micro-batch of data in the writer stream with transformations or actions as needed.
        :param: mock_transformer_executor
        :param: mock_spark_batch_write_file_system
        :param: mock_spark_batch_write_kafka
        :return:
        """
        mock_output_df = MagicMock()
        mock_formulate_metric_dictionary.return_value = self.mock_formulate_metric_dictionary_output
        mock_transformer_executor.return_value = mock_output_df
        mock_spark_batch_write_kafka.return_value = None
        batch_id = 1
        mock_writer_config = {
            "writer_typ": "message_broker_kafka",
            "writer_all_options": {
                "output_mode": "test-mode",
                "write_options": {
                    "kafka.bootstrap.servers": "test_kafka_bootstrap_server",
                    "kafka.security.protocol": "test_security_protocol",
                    "kafka.topic": "test_topic"
                },
            }
        }
        mock_transformer_config = {
            "transformer_key": "value"
        }
        writer_process_micro_batch(
            self.batch_df,
            batch_id,
            mock_writer_config,
            mock_transformer_config,
            self.mock_logger,
            self.mock_splunk_config
        )
        mock_transformer_executor.assert_called_once_with(
            batch_df=self.batch_df,
            spark=self.batch_df.sparkSession,
            config=mock_transformer_config,
            logger=self.mock_logger
        )
        mock_spark_batch_write_file_system.assert_not_called()
        mock_spark_batch_write_kafka.assert_called_once_with(
            input_dataframe=mock_output_df,
            options=mock_writer_config['writer_all_options']['write_options']
        )
        self.mock_logger.info.assert_any_call(f"Batch Transformation Initiated for Micro Batch with ID: {batch_id}")
        self.mock_logger.info.assert_any_call(f"Batch Transformation Completed for Micro Batch with ID: {batch_id}")
        self.mock_logger.info.assert_any_call(f"Batch Write Initiated for Micro Batch with ID: {batch_id}")
        self.mock_logger.info.assert_any_call(f"Batch Write Completed for Micro Batch with ID: {batch_id}")
        self.mock_splunk_builder.build_metrics.assert_called_once_with(
            batch_metrics_list=mock.ANY,
        )
        call_args = self.mock_splunk_builder.build_metrics.call_args[1]
        batch_metrics = call_args['batch_metrics_list']
        self.assertEqual(len(batch_metrics), 2)

        counter_count = sum(1 for metric in batch_metrics if metric['metric_type'] == 'counter')
        gauge_count = sum(1 for metric in batch_metrics if metric['metric_type'] == 'gauge')

        self.assertEqual(counter_count, 1)
        self.assertEqual(gauge_count, 1)

        mock_formulate_metric_dictionary.assert_called_once()
        call_args = mock_formulate_metric_dictionary.call_args[1]
        self.assertEqual(call_args['splunk_metric_config'], self.mock_splunk_config['metric_config'])
        self.assertEqual(call_args['batch_id'], 1),
        assert "microbatch_time" in call_args['actual_metric']
        assert "microbatch_transformation_time" in call_args['actual_metric']
        assert "microbatch_writer_time" in call_args['actual_metric']
        assert "microbatch_writer_record_count" in call_args['actual_metric']
        assert "microbatch_record_count" in call_args['actual_metric']

    @patch('src.main.processor.writer.writer_executor.writer_file_system_params')
    def test_process_writer_configs_file_system(self, mock_writer_file_system_params):
        """
        Test Processes the writer configurations to prepare them for execution with file system writer.
        :param mock_writer_file_system_params:
        :return:
        """
        writer_config = {
            'writer_typ': 'file_system',
            'config-1': 'value-1',
            'config-2': 'value-2',
        }
        expected_output = {
            'file_system': {
                "config-1": "value-1",
                "config-2": "value-2"
            }
        }
        mock_writer_file_system_params.return_value = expected_output
        result = writer_process_configs(writer_config, self.mock_logger)
        mock_writer_file_system_params.assert_called_once_with(
            config=writer_config
        )
        self.assertEqual(result, expected_output)

    @patch('src.main.processor.writer.writer_executor.writer_message_broker_kafka_params')
    def test_writer_process_configs_message_broker_kafka(self, mock_writer_message_broker_kafka_params):
        """
        Test Processes the writer configurations to prepare them for execution with kafka writer.
        :param mock_writer_message_broker_kafka_params:
        :return:
        """
        writer_config = {
            'writer_typ': 'message_broker_kafka',
            'config-1': 'value-1',
            'config-2': 'value-2',
        }
        expected_output = {
            'message_broker_kafka': {
                "config-1": "value-1",
                "config-2": "value-2"
            }
        }
        mock_writer_message_broker_kafka_params.return_value = expected_output
        result = writer_process_configs(
            writer_config=writer_config,
            logger=self.mock_logger
        )
        mock_writer_message_broker_kafka_params.assert_called_once_with(
            config=writer_config
        )
        self.assertEqual(result, expected_output)

    def test_formulate_metric_dictionary(self):
        """
        Test Formulate Standard Microbatch Standard Metric dictionary list compatible to splunk observability cloud
        :return:
        """
        mock_splunk_config: dict = {
            "namespace": "test_namespace",
            "metric_set": {
                "microbatch_time": {
                    "typ": "gauge",
                    "custom_metric_name": "test_microbatch_time"
                },
                "microbatch_transformation_time": {
                    "typ": "gauge",
                    "custom_metric_name": "test_microbatch_transformation_time"
                },
                "microbatch_writer_time": {
                    "typ": "gauge",
                    "custom_metric_name": "test_microbatch_writer_time"
                },
                "microbatch_writer_record_count": {
                    "typ": "counter",
                    "custom_metric_name": "test_microbatch_writer_record_count"
                },
                "microbatch_record_count": {
                    "typ": "counter",
                    "custom_metric_name": "test_microbatch_record_count"
                }
            },
            "flush_interval": -1,
            "flush_batch_size": -1
        }
        mock_batch_id = -1
        mock_actual_metric: dict = {
            "microbatch_time": -1,
            "microbatch_transformation_time": -2,
            "microbatch_writer_time": -3,
            "microbatch_writer_record_count": -4,
            "microbatch_record_count": -5
        }
        expected_result: list[dict] = [
            {
                "metric_type": "gauge",
                "metric": "test_namespace.test_microbatch_time",
                "value": -1,
                "batch_id": -1
            },
            {
                "metric_type": "gauge",
                "metric": "test_namespace.test_microbatch_transformation_time",
                "value": -2,
                "batch_id": -1
            },
            {
                "metric_type": "gauge",
                "metric": "test_namespace.test_microbatch_writer_time",
                "value": -3,
                "batch_id": -1
            },
            {
                "metric_type": "counter",
                "metric": "test_namespace.test_microbatch_writer_record_count",
                "value": -4,
                "batch_id": -1
            },
            {
                "metric_type": "counter",
                "metric": "test_namespace.test_microbatch_record_count",
                "value": -5,
                "batch_id": -1
            }
        ]
        result_metric = formulate_metric_dictionary(
            splunk_metric_config=mock_splunk_config,
            batch_id=mock_batch_id,
            actual_metric=mock_actual_metric
        )
        self.assertEqual(result_metric, expected_result)

    def tearDown(self):
        """
        Tear Down Spark Instance
        :return:
        """
        self.spark.stop()

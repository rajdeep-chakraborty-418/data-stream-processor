"""
Test Class for Processor Executor
"""
# pylint: disable=line-too-long
# pylint: disable=import-error
# pylint: disable=too-few-public-methods
# pylint: disable=unnecessary-pass
# pylint: disable=too-many-arguments

import os
import unittest
from typing import Any
from unittest import (
    mock,
)
from unittest.mock import (
    MagicMock,
)


class TestMain(unittest.TestCase):
    """
    Test Class for Main Function
    """

    def setUp(self):
        """
        Declare Test Config, Arguments and common parameters
        :return:
        """
        self.mock_spark = MagicMock()
        self.mock_input_dataframe = MagicMock()
        self.mock_metric_builder_instance = mock.MagicMock()
        self.mock_logger = mock.MagicMock()
        self.mock_config_obj = mock.Mock()
        self.test_config = {
            'logger_name': 'test_logger',
            'environment': 'test_dev',
            'processor_typ': 'test_processor',
            'aws_region': 'test_region'
        }
        self.mock_reader_output = {
            "reader_typ": "mock_reader",
            "reader_options": {
                "option": "value"
            }
        }

        self.mock_metric_struct = MagicMock()
        self.mock_output_struct = MagicMock()
        self.mock_transform_struct = MagicMock()

        self.mock_config_obj.metric = self.mock_metric_struct
        self.mock_config_obj.output = self.mock_output_struct
        self.mock_config_obj.transform = self.mock_transform_struct
        self.mock_env_var = {
            "CONFIG_FILE_PATH": "/mnt/config.json",
            "REGION": "test_region",
            "CONFIG_READER_SOURCE": "test-reader-source"
        }
        self.expected_writer_dict = {
            "config_reader_source": "test-reader-source",
            "logger_name": "test_logger",
            "environment": "test_dev",
            "processor_typ": "test_processor",
            "aws_region": "test_region",
            "key-1": "value-1",
            "key-2": "value-2",
            "key-options": {
                "key-option-1": "value-option-1"
            }
        }
        self.expected_transformer_dict = {
            "config_reader_source": "test-reader-source",
            "logger_name": "test_logger",
            "environment": "test_dev",
            "processor_typ": "test_processor",
            "aws_region": "test_region",
            "key-1": "transformer-1",
            "key-params": {
                "params-1": "params-value-1"
            }
        }
        self.expected_metric_dict = {
            "flush_interval": -1.00,
            "flush_batch_size": 0,
            "namespace": "test-namespace",
            "metric_set": {
                "test_metric_1": {
                    "typ": "gauge",
                    "custom_metric_name": "test_metric_1_custom"
                },
                "test_metric_2": {
                    "typ": "gauge",
                    "custom_metric_name": "test_metric_2_custom"
                }
            }
        }

    @mock.patch('src.main.processor.executor.reader_stream_executor')
    @mock.patch('src.main.processor.executor.log_end')
    @mock.patch('src.main.processor.executor.log_start')
    @mock.patch('src.main.processor.executor.logger_utils.LoggerUtils.setup_logger')
    @mock.patch('src.main.processor.executor.StructDict')
    @mock.patch('src.main.processor.executor.create_spark_session')
    @mock.patch('src.main.processor.executor.reader_executor')
    @mock.patch('src.main.processor.executor.read_config')
    @mock.patch('src.main.processor.executor.writer_process_configs')
    @mock.patch('src.main.processor.executor.writer_executor')
    @mock.patch('src.main.processor.executor.transformer_process_configs')
    @mock.patch('src.main.processor.executor.read_environment_var')
    @mock.patch('src.main.processor.executor.MetricBuilder')
    def test_main_run(self,
                      mock_metric_builder,
                      mock_read_environment_var,
                      mock_transformer_process_configs,
                      mock_writer_executor,
                      mock_writer_process_configs,
                      mock_read_config,
                      mock_reader_executor,
                      mock_create_spark_session,
                      mock_struct_dict,
                      mock_setup_logger,
                      mock_log_start,
                      mock_log_end,
                      mock_reader_stream_executor):
        """
        Test Main flow happy path
        :param mock_writer_executor:
        :param mock_writer_process_configs:
        :param mock_read_config:
        :param mock_reader_executor:
        :param mock_create_spark_session:
        :param mock_struct_dict:
        :param mock_setup_logger:
        :param mock_log_start:
        :param mock_log_end:
        :param mock_reader_stream_executor:
        :return:
        """
        mock_metric_builder.return_value = self.mock_metric_builder_instance
        mock_splunk_config: dict = {
            "metric_builder": self.mock_metric_builder_instance,
            "metric_config": self.expected_metric_dict
        }
        mock_read_environment_var.return_value = self.mock_env_var
        mock_read_config.return_value = self.test_config
        mock_create_spark_session.return_value = self.mock_spark
        self.mock_input_dataframe.isStreaming = True
        mock_setup_logger.return_value = self.mock_logger
        mock_log_start.return_value = 'start_time'

        self.mock_config_obj.logger_name = self.test_config['logger_name']
        self.mock_config_obj.environment = self.test_config['environment']
        self.mock_config_obj.processor_typ = self.test_config['processor_typ']
        self.mock_config_obj.aws_region = self.test_config['aws_region']
        mock_struct_dict.return_value = self.mock_config_obj

        mock_reader_executor.return_value = self.mock_reader_output
        mock_reader_stream_executor.return_value = self.mock_input_dataframe

        mock_transformer_process_configs.return_value = self.expected_transformer_dict
        mock_writer_executor.return_value = None

        def mock_struct_to_dict(arg):
            """
            Define Mock Struct to Dict for Transformer & Writer
            :param arg:
            :return:
            """
            if arg == self.mock_output_struct:
                return self.expected_writer_dict
            elif arg == self.mock_transform_struct:
                return self.expected_transformer_dict
            elif arg == self.mock_metric_struct:
                return self.expected_metric_dict

        self.mock_config_obj.struct_to_dict.side_effect = mock_struct_to_dict

        mock_writer_process_configs.return_value = self.expected_writer_dict

        from src.main.processor.executor import main
        main()

        mock_setup_logger.assert_called_once_with('test_logger', logging_level=mock.ANY)
        mock_log_start.assert_called_once_with(self.mock_logger)
        mock_metric_builder.assert_called_once_with(
            static_vals={
                "host": "databricks_job",
                "env": self.test_config['environment'],
                "region": self.test_config['aws_region']
            },
            flush_interval_seconds=self.expected_metric_dict['flush_interval'],
            max_batch_size=self.expected_metric_dict['flush_batch_size'],
            logger=self.mock_logger
        )
        mock_reader_executor.assert_called_once_with(
            config=self.mock_config_obj,
            spark=self.mock_spark,
            logger=self.mock_logger
        )
        mock_log_end.assert_called_once_with(
            'start_time',
            self.mock_logger
        )
        mock_reader_stream_executor.assert_called_once_with(
            reader_typ=self.mock_reader_output['reader_typ'],
            reader_typ_options=self.mock_reader_output['reader_options']
        )
        mock_transformer_process_configs.assert_called_once_with(
            transformer_config=self.expected_transformer_dict,
            logger=self.mock_logger
        )
        mock_writer_process_configs.assert_called_once_with(
            writer_config=self.expected_writer_dict,
            logger=self.mock_logger
        )
        mock_writer_executor.assert_called_once_with(
            input_dataframe=self.mock_input_dataframe,
            writer_config=self.expected_writer_dict,
            transformer_config=self.expected_transformer_dict,
            logger=self.mock_logger,
            splunk_config=mock_splunk_config
        )
        self.mock_metric_builder_instance.shutdown.assert_called_once()

"""
Test Transformer Executor Module
"""

# pylint: disable=pointless-string-statement
# pylint: disable=import-error
# pylint: disable=unused-import
# pylint: disable=line-too-long

import unittest
from unittest.mock import (
    patch,
    MagicMock,
    call,
)

from pyspark.sql import (
    DataFrame,
)

from src.main.processor.transformer.transformer_executor import (
    sort_transformation_rule_config,
    transformer_executor,
    transformer_process_configs,
)
from src.main.utils.spark_utils import create_spark_session


class TestSortTransformationRuleConfig(unittest.TestCase):
    """
    Test Sorts the transformation rule configuration based on the order of execution
    """

    def setUp(self):
        """
        Setup Test data
        :return:
        """
        self.input_config_valid = [
            {'name': 'rule1', 'transform_seq': '3'},
            {'name': 'rule2', 'transform_seq': '1'},
            {'name': 'rule3', 'transform_seq': '2'}
        ]

    def test_sort_by_sort_attribute(self):
        """
        Test Sort using a valid attribute
        :return:
        """
        expected_output = [
            {'name': 'rule2', 'transform_seq': '1'},
            {'name': 'rule3', 'transform_seq': '2'},
            {'name': 'rule1', 'transform_seq': '3'}
        ]
        result = sort_transformation_rule_config(self.input_config_valid, 'transform_seq')
        self.assertEqual(result, expected_output)


class TestTransformerExecutor(unittest.TestCase):
    """
    Test Transformer Executor process
    """

    def setUp(self):
        """
        Setup Mock values
        :return:
        """
        self.spark = create_spark_session("test_transformer_executor")
        self.mock_input_df = MagicMock(spec=DataFrame)
        self.mock_output_df = MagicMock(spec=DataFrame)
        self.mock_logger = MagicMock()
        self.config = {
            'config_reader_source': 'test-reader-source',
            'logger_name': 'test_logger',
            'environment': 'test_dev',
            'processor_typ': 'test_processor',
            'aws_region': 'test_region',
            'input': 'input_table',
            'conversion': [
                {
                    'transform_typ': 'sql',
                    'transform_seq': '1',
                    'transform_name': 'test_rule_1',
                    'transform_query': 'select query',
                    'transform_output': 'output_table_1'
                },
                {
                    'transform_typ': 'pyspark',
                    'transform_seq': '2',
                    'transform_name': 'test_rule_2',
                    'transform_query': 'insert query',
                    'transform_output': 'output_table_2'
                }
            ]
        }

    @patch('src.main.processor.transformer.transformer_executor.sql_transformation')
    @patch('src.main.processor.transformer.transformer_executor.pyspark_transformation')
    def test_transformer_executor(self, mock_pyspark_transformation, mock_sql_transformation):
        """
        Test Call the Appropriate Child Transformer Type Method and get the batch dataframe
        :param mock_pyspark_transformation:
        :param mock_sql_transformation:
        :return:
        """
        mock_sql_transformation.return_value = self.mock_output_df
        mock_pyspark_transformation.return_value = self.mock_output_df
        actual_result = transformer_executor(
            self.spark,
            self.mock_input_df,
            self.config,
            self.mock_logger
        )
        self.mock_input_df.createOrReplaceTempView.assert_called_once_with(self.config['input'])
        self.assertEqual(mock_sql_transformation.call_count, 1)
        self.assertEqual(mock_pyspark_transformation.call_count, 1)
        mock_sql_transformation.assert_has_calls([
            call(
                spark=self.spark,
                input_rule_dict=self.config["conversion"][0],
                logger=self.mock_logger
            ),
        ])
        mock_pyspark_transformation.assert_has_calls([
            call(
                spark=self.spark,
                input_rule_dict=self.config["conversion"][1],
                logger=self.mock_logger
            )
        ])
        self.assertEqual(actual_result, self.mock_output_df)

    def tearDown(self):
        """
        Tear Down Spark
        :return:
        """
        self.spark.stop()


class TestTransformerProcessConfigs(unittest.TestCase):
    """
    Test Transformer process configs
    """

    def setUp(self):
        """
        Setup Mock values
        :return:
        """
        self.mock_logger = MagicMock()
        self.transformer_config: dict = {
            'config_reader_source': 'test-reader-source',
            'logger_name': 'test_logger',
            'environment': 'test_dev',
            'processor_typ': 'test_processor',
            'aws_region': 'test_region',
            'input': 'test_input',
            'conversion': [
                {
                    'transform_seq': '1',
                    'transform_typ': 'sql',
                    'transform_name': 'test_sql_transform_1',
                    'transform_input': {
                        'test_config_input_table_1': 'test_input'
                    },
                    'transform_output': {
                        'config_output_table': 'test_1_transform_output'
                    },
                    "transform_query": "/path/query1.sql"
                },
                {
                    'transform_seq': '2',
                    'transform_typ': 'pyspark',
                    'transform_name': 'test_sql_transform_1',
                    'transform_input': {
                        'test_config_input_table_1': 'test_input'
                    },
                    'transform_output': {
                        'config_output_table': 'test_1_transform_output'
                    },
                    "transform_query": "/path/query2.py"
                }
            ],
            'output': 'test_output'
        }

    @patch('src.main.processor.transformer.transformer_executor.sort_transformation_rule_config')
    @patch('src.main.processor.transformer.transformer_executor.read_config')
    def test_transformer_process_configs(self, mock_read_config, mock_sort_transformation_rule_config):
        """
        Test the transformer_process_configs function to ensure it sorts and processes the transformation rules correctly
        for SQL based transformations
        :param mock_read_config:
        :param mock_sort_transformation_rule_config:
        :return:
        """
        mock_sort_transformation_rule_config.return_value = self.transformer_config['conversion']
        mock_read_config.return_value = "Some Statement"
        modified_config: dict = {
            'config_reader_source': 'test-reader-source',
            'logger_name': 'test_logger',
            'environment': 'test_dev',
            'processor_typ': 'test_processor',
            'aws_region': 'test_region',
            'input': 'test_input',
            'conversion': [
                {
                    'transform_seq': '1',
                    'transform_typ': 'sql',
                    'transform_name': 'test_sql_transform_1',
                    'transform_input': {
                        'test_config_input_table_1': 'test_input'
                    },
                    'transform_output': {
                        'config_output_table': 'test_1_transform_output'
                    },
                    "transform_query": "Some Statement"
                },
                {
                    'transform_seq': '2',
                    'transform_typ': 'pyspark',
                    'transform_name': 'test_sql_transform_1',
                    'transform_input': {
                        'test_config_input_table_1': 'test_input'
                    },
                    'transform_output': {
                        'config_output_table': 'test_1_transform_output'
                    },
                    "transform_query": "Some Statement"
                }
            ],
            'output': 'test_output'
        }
        actual_result: dict = transformer_process_configs(
            self.transformer_config,
            self.mock_logger
        )
        mock_sort_transformation_rule_config.assert_called_once_with(
            input_config=self.transformer_config['conversion'],
            sort_attribute='transform_seq'
        )
        mock_read_config.assert_has_calls([
            call(input_region='test_region', input_file_path='/path/query1.sql', input_file_typ='sql', input_config_reader_source='test-reader-source'),
            call(input_region='test_region', input_file_path='/path/query2.py', input_file_typ='pyspark', input_config_reader_source='test-reader-source')
        ])
        self.assertEqual(actual_result, modified_config)

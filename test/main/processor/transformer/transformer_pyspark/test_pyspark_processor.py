"""
Test Transformer Pyspark Rule Type Processor Module
"""
import builtins
# pylint: disable=pointless-string-statement
# pylint: disable=import-error
# pylint: disable=line-too-long


import unittest
from unittest.mock import (
    patch, MagicMock,
)

from pyspark.sql import SparkSession, DataFrame

from src.main.processor.transformer.transformer_pyspark.pyspark_processor import (
    pyspark_transformation,
    prepare_pyspark_namespace,
)


class TestPysparkTransformation(unittest.TestCase):
    """
    Test SQL Rule Processor
    """

    def setUp(self):
        """
        Setup Mocks
        :return:
        """
        self.spark = MagicMock(spec=SparkSession)
        self.mock_df = MagicMock(spec=DataFrame)
        self.logger = MagicMock()
        self.input_rule_dict = {
            'transform_name': 'test_rule',
            'transform_input': {
                'config_input_table_1': 'test_raw_input_1',
                'config_input_table_2': 'test_raw_input_2'
            },
            'transform_output': {
                'config_output_table': 'test_1_transform_output'
            },
            'transform_query': "Some Pyspark Script"
        }

    @patch('src.main.processor.transformer.transformer_pyspark.pyspark_processor.InterfacePysparkScriptExecutor')
    @patch('src.main.processor.transformer.transformer_pyspark.pyspark_processor.prepare_pyspark_namespace')
    def test_pyspark_transformation(self, mock_prepare_pyspark_namespace, mock_interface_pyspark_script_executor):
        """
        test SQL transformation flow
        :param mock_prepare_pyspark_namespace:
        :param mock_interface_pyspark_script_executor:
        :return:
        """
        mock_executor_instance = mock_interface_pyspark_script_executor.return_value
        mock_executor_instance.run.return_value = self.mock_df

        mock_prepare_pyspark_namespace.return_value = {}
        self.spark.read.table.return_value = self.mock_df
        actual_df = pyspark_transformation(
            self.spark,
            self.input_rule_dict,
            self.logger
        )
        self.logger.info.assert_any_call(f"""Transformer Pyspark Module invoked with Rule {self.input_rule_dict['transform_name']}""")
        self.logger.info.assert_any_call(f"""Transformer Pyspark Module completed with Rule {self.input_rule_dict['transform_name']}""")

        mock_interface_pyspark_script_executor.assert_called_once_with(
            self.spark,
            self.input_rule_dict['transform_query'],
            ['test_raw_input_1', 'test_raw_input_2']
        )
        mock_executor_instance.run.assert_called_once()
        self.mock_df.createOrReplaceTempView.assert_called_once_with(
            self.input_rule_dict['transform_output']['config_output_table']
        )
        self.assertEqual(actual_df, self.mock_df)

    def test_prepare_pyspark_namespace(self):
        """
        Test the prepare_pyspark_namespace function
        :return:
        """
        input_variables = {'custom_var': 'value'}
        expected_namespace = {
            '__builtins__': builtins,
            'spark': self.spark,
            'custom_var': 'value'
        }
        actual_namespace = prepare_pyspark_namespace(self.spark, input_variables)
        self.assertEqual(actual_namespace, expected_namespace)

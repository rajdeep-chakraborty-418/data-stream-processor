"""
Test Transformer SQL Rule Type Processor Module
"""
# pylint: disable=pointless-string-statement
# pylint: disable=import-error
# pylint: disable=line-too-long


import unittest
from unittest.mock import (
    patch, MagicMock,
)

from pyspark.sql import SparkSession, DataFrame

from src.main.processor.transformer.transformer_sql.sql_processor import (
    sql_transformation,
)


class TestSQLTransformation(unittest.TestCase):
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
            'transform_query': "Get Data From {config_input_table_1} and {config_input_table_2}"
        }

    @patch('src.main.processor.transformer.transformer_sql.sql_processor.execute_spark_sql')
    def test_sql_transformation(self, mock_execute_spark_sql):
        """
        test SQL transformation flow
        :param mock_execute_spark_sql:
        :return:
        """
        test_formatted_query: str = "Get Data From test_raw_input_1 and test_raw_input_2"
        mock_execute_spark_sql.return_value = self.mock_df
        actual_df = sql_transformation(
            self.spark,
            self.input_rule_dict,
            self.logger
        )
        self.logger.info.assert_any_call(f"""Transformer SQL Module invoked with Rule {self.input_rule_dict['transform_name']}""")
        self.logger.info(f"""Transformer SQL Module invoked with Rule Output Memory {self.input_rule_dict['transform_output']}""")
        self.logger.info.assert_any_call(f"""Transformer SQL Module completed with Rule {self.input_rule_dict['transform_name']}""")
        mock_execute_spark_sql.assert_called_once_with(
            spark=self.spark,
            sql_query=test_formatted_query
        )
        self.mock_df.createOrReplaceTempView.assert_called_once_with(self.input_rule_dict['transform_output']['config_output_table'])
        self.assertEqual(actual_df, self.mock_df)

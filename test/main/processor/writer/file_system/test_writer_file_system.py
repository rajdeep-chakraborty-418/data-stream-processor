"""
Test Writer File System format wise
"""
import unittest

from src.main.processor.writer.file_system.writer_file_system import writer_file_system_params


class TestWriterFileSystemParams(unittest.TestCase):
    """
    Unit tests for the writer_file_system_params function in the file_system module.
    """

    def setUp(self):
        """
        Set up the test case with necessary configurations.
        """
        self.config = {
            'outputMode': 'append',
            'partitionBy': 'date',
            'writer_typ_options': {'option1': 'value1', 'option2': 'value2'},
            'format': 'json',
            'targetPath': '/path/to/output',
            'stream_options': {
                'checkpointLocation': '/path/to/checkpoint'
            }
        }
        self.json_config = {**self.config, **{'format': 'json'}}
        self.avro_config = {**self.config, **{'format': 'avro'}}

    def test_writer_file_system_params_json(self):
        """
        Test the writer_file_system_params function for JSON format.
        :return:
        """

        expected_output = {
            "writer_typ": "file_system",
            "writer_all_options": {
                "write_format": "json",
                "output_mode": "append",
                "partition_by": "date",
                "write_options": {
                    'option1': 'value1',
                    'option2': 'value2'
                },
                "target_path": '/path/to/output'
            },
            'stream_options': {
                'checkpointLocation': '/path/to/checkpoint'
            }
        }

        result = writer_file_system_params(self.json_config)
        self.assertEqual(result, expected_output)

    def test_writer_file_system_params_avro(self):
        """
        Test the writer_file_system_params function for AVRO format.
        :return:
        """

        expected_output = {
            "writer_typ": "file_system",
            "writer_all_options": {
                "write_format": "avro",
                "output_mode": "append",
                "partition_by": "date",
                "write_options": {
                    'option1': 'value1',
                    'option2': 'value2'
                },
                "target_path": '/path/to/output'
            },
            'stream_options': {
                'checkpointLocation': '/path/to/checkpoint'
            }
        }

        result = writer_file_system_params(self.avro_config)
        self.assertEqual(result, expected_output)

"""
File System Reader Implementation Class
"""
# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=too-few-public-methods
# pylint: disable=pointless-string-statement


from src.main.enum.reader_file_type import (
    ReaderFileType,
)
from src.main.model.reader_file_system_model import (
    ReaderFileSystemModel,
)
from src.main.utils.constants import (
    NODE_INPUT_FORMAT,
    NODE_INPUT_SCHEMA,
    NODE_INPUT_FILE_PATTERN,
    NODE_INPUT_SOURCE_PATH,
    NODE_INPUT_FORMAT_OPTIONS,
    NODE_INPUT_GEN_SPARK,
    NODE_INPUT_GEN_OPTIONS,
    NODE_INPUT_GEN_SCHEMA,
    NODE_INPUT_GEN_FORMAT,
    NODE_INPUT_GEN_PATH,
    STRING_TRUE,
    BOOLEAN_TRUE,
    STRING_FALSE,
    BOOLEAN_FALSE,
)
from src.main.utils.spark_utils import (
    generate_spark_schema_from_json,
    generate_spark_schema_from_avro,
)
from src.main.utils.json_utils import (
    get_deep_attributes,
)
from src.main.abstract.abstract_reader import (
    AbstractReader,
)


class ReaderFileSystem(AbstractReader):
    """
    File System Reader Class
    """
    def __init__(self, spark, config):
        """
        Constructor for Reader File System Class
        :param spark:
        :param config:
        """
        super().__init__(spark, config)
        self.file_format = get_deep_attributes(config.input, [NODE_INPUT_FORMAT])
        self.input_schema = get_deep_attributes(config.input, [NODE_INPUT_SCHEMA]).items()
        self.source_path = get_deep_attributes(config.input, [NODE_INPUT_SOURCE_PATH])
        self.file_pattern = get_deep_attributes(config.input, [NODE_INPUT_FILE_PATTERN])
        """
        Options is specific to the file format
        """
        self.format_options = get_deep_attributes(config.input, [NODE_INPUT_FORMAT_OPTIONS]).items()
        """
        Convert a StructDict object into a proper dictionary
        """
        self.input_schema_dict: dict = dict(self.input_schema)

    def reader_get(self) -> dict:
        """
        Perform all the operation for ReaderFileSystem
        :return:
        """
        return self._reader_set()

    def _reader_set(self) -> dict:
        """
        Set the Reader Options, Format, Schema
        :return:
        """
        """
        Initialise the model class
        """
        reader_model = ReaderFileSystemModel()
        """
        Assign Model Class key's for caller
        """
        reader_model[NODE_INPUT_GEN_SPARK] = self.spark
        reader_model[NODE_INPUT_GEN_OPTIONS] = self._reader_get_options()[NODE_INPUT_GEN_OPTIONS]
        reader_model[NODE_INPUT_GEN_SCHEMA] = self._reader_get_schema()[NODE_INPUT_GEN_SCHEMA]
        reader_model[NODE_INPUT_GEN_FORMAT] = self._reader_get_format()[NODE_INPUT_GEN_FORMAT]
        reader_model[NODE_INPUT_GEN_PATH] = self.__reader_get_file_path()[NODE_INPUT_GEN_PATH]

        return reader_model.get_all_data()

    def _reader_get_format(self) -> dict:
        """
        Get the Format for the Reader
        :return:
        """
        return {
            NODE_INPUT_GEN_FORMAT: self.file_format
        }

    def __reader_get_file_path(self) -> dict:
        """
        Get the Absolute Path for the Reader
        :return:
        """
        return {
            NODE_INPUT_GEN_PATH: (
                    self.source_path
                    +
                    self.file_pattern
            )
        }

    def _reader_get_options(self) -> dict:
        """
        Generate the Options for the Reader
        :return:
        """
        options = self.__reader_format_reader_options()
        return {
            NODE_INPUT_GEN_OPTIONS: options,
        }

    def _reader_get_schema(self) -> dict:
        """
        Get the Schema for the Reader
        :return:
        """
        spark_schema = None

        match self.file_format:

            case ReaderFileType.JSON.value:
                """
                JSON Specific Conversions
                    Schema Conversion for Json format to Spark StructType
                """
                spark_schema = generate_spark_schema_from_json(
                    self.input_schema_dict
                )

            case ReaderFileType.AVRO.value:
                """
                AVRO Specific Conversions
                    Schema Conversion for AVRO format to Spark StructType
                """
                spark_schema = generate_spark_schema_from_avro(
                    self.input_schema_dict
                )

        return {
            NODE_INPUT_GEN_SCHEMA: spark_schema
        }

    def __reader_format_reader_options(self):
        """
        Generate Options based on format_options provided
        :return:
        """
        formatted_options = {}
        for key, value in self.format_options:
            """
            Parse through all format options
            """
            if str(value).lower() == STRING_TRUE:
                formatted_options[key] = BOOLEAN_TRUE
            elif str(value).lower() == STRING_FALSE:
                formatted_options[key] = BOOLEAN_FALSE
            else:
                formatted_options[key] = value

        return formatted_options

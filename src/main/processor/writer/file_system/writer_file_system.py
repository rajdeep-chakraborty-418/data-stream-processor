"""
Writer File System format wise
"""
# pylint: disable=pointless-string-statement
# pylint: disable=import-error

from src.main.enum.writer_file_type import (
    WriterFileType,
)
from src.main.enum.writer_type import (
    WriterType,
)
from src.main.utils.constants import (
    NODE_OUTPUT_OUTPUT_MODE,
    NODE_OUTPUT_PARTITION_BY,
    NODE_OUTPUT_WRITER_TYP_OPTIONS,
    NODE_OUTPUT_TARGET_PATH,
    NODE_OUTPUT_STREAM_OPTIONS,
    NODE_OUTPUT_FORMAT,
    NODE_OUTPUT_GEN_WRITER_TYP,
    NODE_OUTPUT_GEN_WRITER_ALL_OPTIONS,
    NODE_OUTPUT_GEN_WRITE_FORMAT,
    NODE_OUTPUT_GEN_OUTPUT_MODE,
    NODE_OUTPUT_GEN_PARTITION_BY,
    NODE_OUTPUT_GEN_WRITE_OPTIONS,
    NODE_OUTPUT_GEN_TARGET_PATH,
    NODE_OUTPUT_GEN_STREAM_OPTIONS,
)


# pylint: disable=pointless-string-statement
# pylint: disable=line-too-long
# pylint: disable=import-error

def writer_file_system_params(config: dict) -> dict:
    """
    Get the file system writer Parameters for the given configuration.
    :param config:
    :return:
    """
    output_mode: str = config[NODE_OUTPUT_OUTPUT_MODE]
    partition_by: str = config[NODE_OUTPUT_PARTITION_BY]
    options: dict = config[NODE_OUTPUT_WRITER_TYP_OPTIONS]
    target_path: str = config[NODE_OUTPUT_TARGET_PATH]
    stream_options: dict = config[NODE_OUTPUT_STREAM_OPTIONS]
    """
    Unified Parse Options for File System Writer
    """
    parse_options: dict = {
        NODE_OUTPUT_GEN_WRITER_TYP: WriterType.FILE_SYSTEM.value,
        NODE_OUTPUT_GEN_WRITER_ALL_OPTIONS: {
            NODE_OUTPUT_GEN_WRITE_FORMAT: "",
            NODE_OUTPUT_GEN_OUTPUT_MODE: output_mode,
            NODE_OUTPUT_GEN_PARTITION_BY: partition_by,
            NODE_OUTPUT_GEN_WRITE_OPTIONS: options,
            NODE_OUTPUT_GEN_TARGET_PATH: target_path
        },
        NODE_OUTPUT_GEN_STREAM_OPTIONS: stream_options,
    }
    """
    For Specific File Formats add custom values 
    """
    match config[NODE_OUTPUT_FORMAT]:

        case WriterFileType.JSON.value:
            parse_options[NODE_OUTPUT_GEN_WRITER_ALL_OPTIONS][NODE_OUTPUT_GEN_WRITE_FORMAT] = WriterFileType.JSON.value

        case WriterFileType.AVRO.value:
            parse_options[NODE_OUTPUT_GEN_WRITER_ALL_OPTIONS][NODE_OUTPUT_GEN_WRITE_FORMAT] = WriterFileType.AVRO.value

    return parse_options

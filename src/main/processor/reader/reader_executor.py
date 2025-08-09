"""
Reader Entrypoint Methods
"""
# pylint: disable=import-error
# pylint: disable=too-few-public-methods
# pylint: disable=pointless-string-statement
# pylint: disable=line-too-long

from pyspark.sql import DataFrame

from src.main.enum.message_broker_reader_format_type import MessageBrokerReaderFormatType
from src.main.enum.reader_type import (
    ReaderType,
)
from src.main.processor.reader.file_system.reader_file_system import (
    ReaderFileSystem,
)
from src.main.processor.reader.message_broker.kafka.reader_message_broker_kafka import (
    ReaderMessageBrokerKafka,
)
from src.main.utils.constants import (
    NODE_INPUT_READER_TYP,
    NODE_INPUT_GEN_READER_OPTIONS,
    NODE_INPUT_GEN_SPARK,
    NODE_INPUT_GEN_FORMAT,
    NODE_INPUT_GEN_OPTIONS,
    NODE_INPUT_GEN_SCHEMA,
    NODE_INPUT_GEN_PATH,
)
from src.main.utils.json_utils import (
    get_deep_attributes,
)
from src.main.utils.spark_stream_utils import (
    spark_read_file_system_stream, spark_read_kafka_stream,
)
from src.main.utils.spark_utils import format_kafka_json_dataframe, format_kafka_avro_dataframe


def reader_executor(spark, config, logger) -> dict:
    """
    Call the Appropriate Child Reader Method and get the streaming dataframe
    :param logger:
    :param spark:
    :param config:
    :return:
    """
    """
    Get the reader type from Config
    """
    reader_typ = get_deep_attributes(config.input, [NODE_INPUT_READER_TYP])
    logger.info(f"""Reader Implementation Module {reader_typ} will be invoked""")
    reader_typ_options = {}
    match reader_typ:

        case ReaderType.FILE_SYSTEM.value:
            """
            Reader Type File System Stream Generated Options Method
            """
            child_processor = ReaderFileSystem(spark, config)
            reader_typ_options = child_processor.reader_get()

        case ReaderType.MESSAGE_BROKER_KAFKA.value:
            """
            Reader Type Message Broker Kafka Stream Generated Options Method
            """
            child_processor = ReaderMessageBrokerKafka(spark, config, logger)
            reader_typ_options = child_processor.reader_get()

    return {
        NODE_INPUT_READER_TYP: reader_typ,
        NODE_INPUT_GEN_READER_OPTIONS: reader_typ_options
    }


def reader_stream_executor(reader_typ: str, reader_typ_options: dict) -> DataFrame:
    """
    Reader Stream Executor Method to handle different reader types
    :param reader_typ:
    :param reader_typ_options:
    :return:
    """
    stream_dataframe = None
    match reader_typ:

        case ReaderType.FILE_SYSTEM.value:
            """
            Reader Type File System Stream Executor Method
            """
            stream_dataframe = spark_read_file_system_stream(
                spark=reader_typ_options.get(NODE_INPUT_GEN_SPARK),
                file_format=reader_typ_options.get(NODE_INPUT_GEN_FORMAT),
                options=reader_typ_options.get(NODE_INPUT_GEN_OPTIONS),
                schema=reader_typ_options.get(NODE_INPUT_GEN_SCHEMA),
                path=reader_typ_options.get(NODE_INPUT_GEN_PATH)
            )

        case ReaderType.MESSAGE_BROKER_KAFKA.value:
            """
            Reader Type Message Broker Kafka Stream Executor Method
            """
            stream_dataframe = spark_read_kafka_stream(
                spark=reader_typ_options.get(NODE_INPUT_GEN_SPARK),
                options=reader_typ_options.get(NODE_INPUT_GEN_OPTIONS)
            )

            match reader_typ_options.get(NODE_INPUT_GEN_FORMAT):

                case MessageBrokerReaderFormatType.JSON.value:
                    """
                    Format Kafka Message Broker JSON DataFrame Method
                    """
                    stream_dataframe = format_kafka_json_dataframe(
                        input_dataframe=stream_dataframe,
                        input_schema=reader_typ_options.get(NODE_INPUT_GEN_SCHEMA)
                    )

                case MessageBrokerReaderFormatType.AVRO.value:
                    """
                    Format Kafka Message Broker AVRO DataFrame Method
                    """
                    stream_dataframe = format_kafka_avro_dataframe(
                        input_dataframe=stream_dataframe,
                        input_schema=reader_typ_options.get(NODE_INPUT_GEN_SCHEMA)
                    )

    return stream_dataframe

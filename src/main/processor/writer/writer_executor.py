"""
Writer Executor Module
"""

# pylint: disable=pointless-string-statement
# pylint: disable=line-too-long
# pylint: disable=import-error

from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from src.main.enum.writer_type import (
    WriterType,
)
from src.main.observability.splunk.enum.splunk_metric_typ import (
    SplunkMetricType,
)
from src.main.observability.splunk.http_event_collector.utils.constants import (
    METRIC_NODE_METRIC_TYPE,
    METRIC_NODE_METRIC_VALUE,
    METRIC_NODE_METRIC_BATCH_ID,
    METRIC_NODE_METRIC,
)
from src.main.processor.transformer.transformer_executor import (
    transformer_executor,
)
from src.main.processor.writer.file_system.writer_file_system import (
    writer_file_system_params,
)
from src.main.processor.writer.message_broker.kafka.writer_message_broker_kafka import (
    writer_message_broker_kafka_params,
)
from src.main.utils.constants import (
    NODE_OUTPUT_WRITER_TYP,
    NODE_OUTPUT_GEN_STREAM_OPTIONS,
    NODE_OUTPUT_GEN_WRITER_ALL_OPTIONS,
    NODE_OUTPUT_GEN_OUTPUT_MODE,
    NODE_OUTPUT_GEN_WRITER_TYP,
    NODE_OUTPUT_GEN_PARTITION_BY,
    NODE_OUTPUT_GEN_WRITE_OPTIONS,
    NODE_OUTPUT_GEN_WRITE_FORMAT,
    NODE_OUTPUT_GEN_TARGET_PATH,
    NODE_SPLUNK_METRIC_BUILDER,
    NODE_SPLUNK_METRIC_CONFIG_METRIC_SET,
    NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_TIME,
    NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_TRANSFORMATION_TIME,
    NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_WRITER_TIME,
    NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_WRITER_RECORD_COUNT,
    NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_RECORD_COUNT,
    NODE_SPLUNK_METRIC_CONFIG_METRIC_CUSTOM_METRIC_NAME,
    NODE_SPLUNK_METRIC_NAMESPACE,
    NODE_SPLUNK_METRIC_CONFIG, NODE_SPLUNK_METRIC_CONFIG_METRIC_TYPE,
)
from src.main.utils.spark_utils import (
    spark_batch_write_file_system,
    spark_batch_write_kafka,
)
from src.main.utils.time_utils import (
    get_current_datetime,
    get_time_diff,
)


def formulate_metric_dictionary(splunk_metric_config: dict, batch_id: int, actual_metric: dict) -> list[dict]:
    """
    Formulate Standard Microbatch Standard Metric dictionary list compatible to splunk observability cloud
    :param: splunk_metric_config
    :param: batch_id
    :param: actual_metric
    :return:
    """
    metric_list = [
        {
            METRIC_NODE_METRIC_TYPE: splunk_metric_config[NODE_SPLUNK_METRIC_CONFIG_METRIC_SET][NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_TIME][NODE_SPLUNK_METRIC_CONFIG_METRIC_TYPE],
            METRIC_NODE_METRIC:
                f"""{splunk_metric_config[NODE_SPLUNK_METRIC_NAMESPACE]}.{splunk_metric_config[NODE_SPLUNK_METRIC_CONFIG_METRIC_SET][NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_TIME][
                    NODE_SPLUNK_METRIC_CONFIG_METRIC_CUSTOM_METRIC_NAME]}""",
            METRIC_NODE_METRIC_VALUE: actual_metric[NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_TIME],
            METRIC_NODE_METRIC_BATCH_ID: batch_id
        },
        {
            METRIC_NODE_METRIC_TYPE: splunk_metric_config[NODE_SPLUNK_METRIC_CONFIG_METRIC_SET][NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_TRANSFORMATION_TIME][NODE_SPLUNK_METRIC_CONFIG_METRIC_TYPE],
            METRIC_NODE_METRIC: f"""{splunk_metric_config[NODE_SPLUNK_METRIC_NAMESPACE]}.{splunk_metric_config[NODE_SPLUNK_METRIC_CONFIG_METRIC_SET][
                NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_TRANSFORMATION_TIME][
                NODE_SPLUNK_METRIC_CONFIG_METRIC_CUSTOM_METRIC_NAME]}""",
            METRIC_NODE_METRIC_VALUE: actual_metric[NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_TRANSFORMATION_TIME],
            METRIC_NODE_METRIC_BATCH_ID: batch_id
        },
        {
            METRIC_NODE_METRIC_TYPE: splunk_metric_config[NODE_SPLUNK_METRIC_CONFIG_METRIC_SET][NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_WRITER_TIME][NODE_SPLUNK_METRIC_CONFIG_METRIC_TYPE],
            METRIC_NODE_METRIC: f"""{splunk_metric_config[NODE_SPLUNK_METRIC_NAMESPACE]}.{splunk_metric_config[NODE_SPLUNK_METRIC_CONFIG_METRIC_SET][
                NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_WRITER_TIME][
                NODE_SPLUNK_METRIC_CONFIG_METRIC_CUSTOM_METRIC_NAME]}""",
            METRIC_NODE_METRIC_VALUE: actual_metric[NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_WRITER_TIME],
            METRIC_NODE_METRIC_BATCH_ID: batch_id
        },
        {
            METRIC_NODE_METRIC_TYPE: splunk_metric_config[NODE_SPLUNK_METRIC_CONFIG_METRIC_SET][NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_WRITER_RECORD_COUNT][NODE_SPLUNK_METRIC_CONFIG_METRIC_TYPE],
            METRIC_NODE_METRIC: f"""{splunk_metric_config[NODE_SPLUNK_METRIC_NAMESPACE]}.{splunk_metric_config[NODE_SPLUNK_METRIC_CONFIG_METRIC_SET][
                NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_WRITER_RECORD_COUNT][
                NODE_SPLUNK_METRIC_CONFIG_METRIC_CUSTOM_METRIC_NAME]}""",
            METRIC_NODE_METRIC_VALUE: actual_metric[NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_WRITER_RECORD_COUNT],
            METRIC_NODE_METRIC_BATCH_ID: batch_id
        },
        {
            METRIC_NODE_METRIC_TYPE: splunk_metric_config[NODE_SPLUNK_METRIC_CONFIG_METRIC_SET][NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_RECORD_COUNT][NODE_SPLUNK_METRIC_CONFIG_METRIC_TYPE],
            METRIC_NODE_METRIC: f"""{splunk_metric_config[NODE_SPLUNK_METRIC_NAMESPACE]}.{splunk_metric_config[NODE_SPLUNK_METRIC_CONFIG_METRIC_SET][
                NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_RECORD_COUNT][
                NODE_SPLUNK_METRIC_CONFIG_METRIC_CUSTOM_METRIC_NAME]}""",
            METRIC_NODE_METRIC_VALUE: actual_metric[NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_RECORD_COUNT],
            METRIC_NODE_METRIC_BATCH_ID: batch_id
        }
    ]
    return metric_list


def writer_process_micro_batch(batch_df, batch_id, writer_config, transformer_config, logger, splunk_config):
    """
    Processes a micro-batch of data in the writer stream with transformations or actions as needed.
    :param batch_df:
    :param batch_id:
    :param writer_config:
    :param transformer_config:
    :param logger:
    :param splunk_config:
    :return:
    """
    microbatch_time_start: datetime = get_current_datetime()
    microbatch_record_count: int = batch_df.count()
    if microbatch_record_count == 0:
        return
    logger.info(f"""No Of Incoming Records {microbatch_record_count} for Micro Batch with ID: {batch_id}""")
    logger.info(f"""Batch Transformation Initiated for Micro Batch with ID: {batch_id}""")
    spark: SparkSession = batch_df.sparkSession
    """
    Call the transformer executor with microbatch dataframe & config
    """
    microbatch_transformation_time_start: datetime = get_current_datetime()
    output_dataframe: DataFrame = transformer_executor(
        batch_df=batch_df,
        spark=spark,
        config=transformer_config,
        logger=logger
    )
    microbatch_transformation_time_end: datetime = get_current_datetime()
    microbatch_transformation_time = get_time_diff(
        microbatch_transformation_time_start,
        microbatch_transformation_time_end
    )['milliseconds']
    microbatch_writer_record_count: int = output_dataframe.count()
    logger.info(f"""Batch Transformation Completed for Micro Batch with ID: {batch_id}""")
    logger.info(f"""No Of Outgoing Records {microbatch_writer_record_count} for Micro Batch with ID: {batch_id}""")
    logger.info(f"""Batch Write Initiated for Micro Batch with ID: {batch_id}""")
    """
    Based on Writer Type Invoke specific batch write method
    """
    microbatch_writer_time_start: datetime = get_current_datetime()
    match writer_config[NODE_OUTPUT_GEN_WRITER_TYP]:

        case WriterType.FILE_SYSTEM.value:
            """
            File System Writer Logic
            """
            writer_all_options: dict = writer_config[NODE_OUTPUT_GEN_WRITER_ALL_OPTIONS]
            spark_batch_write_file_system(
                input_dataframe=output_dataframe,
                output_mode=writer_all_options[NODE_OUTPUT_GEN_OUTPUT_MODE],
                partition_columns=writer_all_options[NODE_OUTPUT_GEN_PARTITION_BY],
                options=writer_all_options[NODE_OUTPUT_GEN_WRITE_OPTIONS],
                file_format=writer_all_options[NODE_OUTPUT_GEN_WRITE_FORMAT],
                output_path=writer_all_options[NODE_OUTPUT_GEN_TARGET_PATH]
            )

        case WriterType.MESSAGE_BROKER_KAFKA.value:
            """
            Message Broker Writer Logic
            """
            writer_all_options: dict = writer_config[NODE_OUTPUT_GEN_WRITER_ALL_OPTIONS]
            spark_batch_write_kafka(
                input_dataframe=output_dataframe,
                options=writer_all_options[NODE_OUTPUT_GEN_WRITE_OPTIONS],
            )

    microbatch_writer_time_end: datetime = get_current_datetime()
    microbatch_writer_time = get_time_diff(
        microbatch_writer_time_start,
        microbatch_writer_time_end
    )['milliseconds']
    logger.info(f"""Batch Write Completed for Micro Batch with ID: {batch_id}""")
    microbatch_time_end: datetime = get_current_datetime()
    microbatch_time = get_time_diff(
        microbatch_time_start,
        microbatch_time_end
    )['milliseconds']
    """
    Push Splunk Metrics in a separate thread Pool
    """
    logger.info(f"""Metric Composition for Micro Batch with ID: {batch_id}""")
    metric_list = formulate_metric_dictionary(
        splunk_metric_config=splunk_config[NODE_SPLUNK_METRIC_CONFIG],
        batch_id=batch_id,
        actual_metric={
            NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_TIME: microbatch_time,
            NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_TRANSFORMATION_TIME: microbatch_transformation_time,
            NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_WRITER_TIME: microbatch_writer_time,
            NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_WRITER_RECORD_COUNT: microbatch_writer_record_count,
            NODE_SPLUNK_METRIC_CONFIG_METRIC_MICROBATCH_RECORD_COUNT: microbatch_record_count
        }
    )
    """
    Invoke Splunk Metric Builder Method In Thread Safe Process Asynchronously
    """
    splunk_config[NODE_SPLUNK_METRIC_BUILDER].build_metrics(
        batch_metrics_list=metric_list
    )


def writer_process_configs(writer_config: dict, logger) -> dict:
    """
    Processes the writer configurations to prepare them for execution.
    :param writer_config:
    :param logger:
    :return:
    """
    """
    Invoke the writer logic with the output dataframe and config
    """
    writer_options: dict = {}
    match writer_config[NODE_OUTPUT_WRITER_TYP]:

        case WriterType.FILE_SYSTEM.value:
            """
            File System Config Retrieval
            """
            writer_options = writer_file_system_params(
                config=writer_config,
            )

        case WriterType.MESSAGE_BROKER_KAFKA.value:
            """
            Message Broker Kafka Config Retrieval
            """
            writer_options = writer_message_broker_kafka_params(
                config=writer_config
            )

    return writer_options


def writer_executor(input_dataframe: DataFrame, writer_config, transformer_config, logger, splunk_config) -> None:
    """
    Executes the writer logic for the given input DataFrame and configuration.
    :param input_dataframe:
    :param writer_config:
    :param transformer_config:
    :param logger:
    :param splunk_config:
    :return:
    """
    stream_options: dict = writer_config[NODE_OUTPUT_GEN_STREAM_OPTIONS]
    """
    Writer Stream Initialization with micro batch process
    """
    query = input_dataframe.writeStream \
        .foreachBatch(
        lambda batch_df, batch_id: writer_process_micro_batch(
            batch_df,
            batch_id,
            writer_config,
            transformer_config,
            logger,
            splunk_config)
    ) \
        .outputMode(writer_config[NODE_OUTPUT_GEN_WRITER_ALL_OPTIONS][NODE_OUTPUT_GEN_OUTPUT_MODE]) \
        .options(**stream_options) \
        .start()

    query.awaitTermination()

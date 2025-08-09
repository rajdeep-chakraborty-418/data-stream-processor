"""
Entrypoint Module for Dynamo Loader
"""
# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=too-few-public-methods
# pylint: disable=pointless-string-statement

import logging

from src.main.enum.config_file_type import (
    ConfigFileType,
)
from src.main.observability.splunk.http_event_collector.builder.metric_builder import (
    MetricBuilder,
)
from src.main.observability.splunk.http_event_collector.utils.constants import (
    METRIC_NODE_METRIC_ENV,
    METRIC_NODE_METRIC_REGION,
    METRIC_NODE_HOST,
)
from src.main.processor.transformer.transformer_executor import (
    transformer_process_configs,
)
from src.main.processor.writer.writer_executor import (
    writer_executor,
    writer_process_configs,
)
from src.main.utils.constants import (
    PLEASE_CHECK_CONFIGURATION,
    NODE_INPUT_READER_TYP,
    NODE_INPUT_GEN_READER_OPTIONS,
    NODE_AWS_REGION,
    NODE_ENVIRONMENT,
    NODE_LOGGER_NAME,
    NODE_PROCESSOR_TYP,
    ENV_VAR_CONFIG_FILE_PATH,
    ENV_VAR_REGION,
    ENV_VAR_CONFIG_READER_SOURCE,
    NODE_CONFIG_READER_SOURCE,
    NODE_SPLUNK_METRIC_BUILDER,
    NODE_METRIC_FLUSH_BATCH_SIZE,
    NODE_METRIC_FLUSH_INTERVAL, NODE_SPLUNK_METRIC_CONFIG,
)

from src.main.utils.processor_utils import (
    read_environment_var,
    read_config,
)
from src.main.utils.spark_utils import (
    create_spark_session,
)
from src.main.processor.reader.reader_executor import (
    reader_executor, reader_stream_executor,
)
from src.main.error.codes.error_codes import (
    ALARM_STREAM_PROCESSOR_UNHANDLED_EXCEPTION,
    ALARM_STREAM_PROCESSOR_STREAM_DATAFRAME_EXCEPTION,
)
from src.main.utils.json_utils import (
    StructDict,
)
from src.main.utils.logger_utils import (
    log_start,
    log_end,
)
from src.main.utils import (
    logger_utils,
)


def main():
    """
    Main Entrypoint Method
    :return:
    """
    spark = create_spark_session('PlatformProcessor')
    """
    Assign Input Environment variables
    """
    input_env_vars: dict = read_environment_var()
    input_config_file_path: str = input_env_vars[ENV_VAR_CONFIG_FILE_PATH]
    input_region: str = input_env_vars[ENV_VAR_REGION]
    input_config_reader_source: str = input_env_vars[ENV_VAR_CONFIG_READER_SOURCE]
    """
    Read Config File Based on Config Reader Source
    """
    json_config = read_config(
        input_region=input_region,
        input_file_path=input_config_file_path,
        input_file_typ=ConfigFileType.JSON.value,
        input_config_reader_source=input_config_reader_source
    )
    config = StructDict(**json_config)
    logger = logger_utils.LoggerUtils.setup_logger(config.logger_name, logging_level=logging.INFO)
    start_time = log_start(logger)
    splunk_config: dict = {}
    try:
        logger.info(
            f"""Processing Initiated for Environment {config.environment} for Processor {config.processor_typ}"""
        )
        """
        Build a Common Dictionary with Process level config to 
        add with Input / Output / Transform nodes if required
        """
        process_config = {
            NODE_CONFIG_READER_SOURCE: input_config_reader_source,
            NODE_AWS_REGION: input_region,
            NODE_ENVIRONMENT: config.environment,
            NODE_PROCESSOR_TYP: config.processor_typ,
            NODE_LOGGER_NAME: config.logger_name
        }
        """
        Initiate a Splunk Metric Builder Instance with Static Dimensions
        and Flush Interval & Batch Size
        """
        metric_config: dict = config.struct_to_dict(config.metric)
        metric_builder = MetricBuilder(
            static_vals={
                METRIC_NODE_HOST: "databricks_job",
                METRIC_NODE_METRIC_ENV: process_config[NODE_ENVIRONMENT],
                METRIC_NODE_METRIC_REGION: process_config[NODE_AWS_REGION]
            },
            flush_interval_seconds=metric_config[NODE_METRIC_FLUSH_INTERVAL],
            max_batch_size=metric_config[NODE_METRIC_FLUSH_BATCH_SIZE],
            logger=logger,
        )
        splunk_config: dict = {
            NODE_SPLUNK_METRIC_BUILDER: metric_builder,
            NODE_SPLUNK_METRIC_CONFIG: metric_config
        }
        """
        """
        input_options = reader_executor(
            config=config,
            spark=spark,
            logger=logger
        )
        logger.info(
            f"""Reader Module {input_options[NODE_INPUT_READER_TYP]} invoked"""
        )
        input_dataframe = reader_stream_executor(
            reader_typ=input_options[NODE_INPUT_READER_TYP],
            reader_typ_options=input_options[NODE_INPUT_GEN_READER_OPTIONS]
        )
        logger.info(
            f"""Streaming Input Dataframe (Yes/No) is {input_dataframe.isStreaming}"""
        )
        if not input_dataframe.isStreaming:
            logger.error(ALARM_STREAM_PROCESSOR_STREAM_DATAFRAME_EXCEPTION + " %s", PLEASE_CHECK_CONFIGURATION)
            raise

        """
        Writer Options convertion to standard dictionary format
        """
        writer_config: dict = {
            **process_config, **config.struct_to_dict(config.output)
        }
        """
        Convert Writer config to standard dictionary format compatible to stream write
        per output type and format
        """
        writer_config = writer_process_configs(
            writer_config=writer_config,
            logger=logger
        )
        """
        Transformer Options convertion to standard dictionary format
        """
        transformer_config: dict = {
            **process_config, **config.struct_to_dict(config.transform)
        }
        """
        Convert Transformer config to parse logic files to get content and sort transformations
        """
        transformer_config = transformer_process_configs(
            transformer_config=transformer_config,
            logger=logger
        )
        """
        Invoke the micro batch streaming process
        """
        logger.info(
            f"""Writer Module {input_options[NODE_INPUT_READER_TYP]} invoked"""
        )
        writer_executor(
            input_dataframe=input_dataframe,
            writer_config=writer_config,
            transformer_config=transformer_config,
            logger=logger,
            splunk_config=splunk_config
        )

    except Exception as ex:
        logger.error(ALARM_STREAM_PROCESSOR_UNHANDLED_EXCEPTION + " %s", str(ex))
        raise

    finally:
        log_end(start_time, logger)
        splunk_config[NODE_SPLUNK_METRIC_BUILDER].shutdown()


if __name__ == '__main__':
    main()

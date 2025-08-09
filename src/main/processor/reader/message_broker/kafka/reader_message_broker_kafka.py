"""
Message Broker Kafka Reader Implementation Class
"""

# pylint: disable=import-error
# pylint: disable=line-too-long
# pylint: disable=too-few-public-methods
# pylint: disable=pointless-string-statement

from src.main.abstract.abstract_reader import (
    AbstractReader,
)
from src.main.enum.reader_file_type import (
    ReaderFileType,
)
from src.main.model.reader_message_broker_model import (
    ReaderMessageBrokerModel,
)
from src.main.utils.aws_utils.secret_utils import (
    SecretUtils,
)
from src.main.utils.constants import (
    NODE_INPUT_GEN_SPARK,
    NODE_INPUT_GEN_OPTIONS,
    NODE_INPUT_GEN_SCHEMA,
    NODE_INPUT_GEN_FORMAT,
    NODE_INPUT_FORMAT,
    NODE_INPUT_FORMAT_OPTIONS,
    NODE_INPUT_SCHEMA,
    NODE_INPUT_KAFKA_SECRET_NAME,
    NODE_INPUT_FORMAT_OPTIONS_KAFKA_BOOTSTRAP_SERVER,
    NODE_INPUT_FORMAT_OPTIONS_KAFKA_SECURITY_PROTOCOL,
    NODE_INPUT_FORMAT_OPTIONS_KAFKA_SOURCE_TOPIC,
    NODE_INPUT_FORMAT_OPTIONS_KAFKA_SECRET_KEY_STORE_JKS,
    NODE_INPUT_FORMAT_OPTIONS_KAFKA_SECRET_KEY_STORE_PASSWORD,
    NODE_INPUT_FORMAT_OPTIONS_KAFKA_SECRET_TRUST_STORE_JKS,
    NODE_INPUT_FORMAT_OPTIONS_KAFKA_SECRET_TRUST_STORE_PASSWORD,
    NODE_INPUT_FORMAT_OPTIONS_KAFKA_SECRET_SSL_KEY_PASSWORD,
    NODE_INPUT_FORMAT_OPTIONS_KAFKA_SECRET_STARTING_OFFSET,
    NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_BOOTSTRAP_SERVER,
    NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SECURITY_PROTOCOL,
    NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SOURCE_TOPIC,
    NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SECRET_KEY_STORE_JKS,
    NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SECRET_KEY_STORE_PASSWORD,
    NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SECRET_TRUST_STORE_JKS,
    NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SECRET_TRUST_STORE_PASSWORD,
    NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SECRET_SSL_KEY_PASSWORD,
    NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SECRET_STARTING_OFFSET,
    NODE_INPUT_KAFKA_SECRET_NAME,
    NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SECRET_FAIL_ON_DATA_LOSS,
    NODE_INPUT_FORMAT_OPTIONS_KAFKA_SECRET_FAIL_ON_DATA_LOSS,

)
from src.main.utils.spark_utils import (
    generate_spark_schema_from_json,
    generate_spark_schema_from_avro,
)


class ReaderMessageBrokerKafka(AbstractReader):
    """
    Message Broker Reader Class
    """

    def __init__(self, spark, config, logger):
        """
        Constructor for Message Broker Class
        :param spark:
        :param config:
        :param logger:
        """
        super().__init__(spark, config)
        self.logger = logger
        self.config_dict = config.struct_to_dict(config.input)
        self.broker_data_format = self.config_dict[NODE_INPUT_FORMAT]
        self.broker_secret = self.config_dict[NODE_INPUT_KAFKA_SECRET_NAME]
        self.broker_options = self.config_dict[NODE_INPUT_FORMAT_OPTIONS]
        self.input_schema_dict: dict = self.config_dict[NODE_INPUT_SCHEMA]
        """
        Get the secret from AWS Secrets Manager
        """
        self.secret_instance = SecretUtils(
            region_name=self.aws_region,
            logger=self.logger
        )
        self.secret = self.secret_instance.get_secret(
            secret_name=self.broker_secret,
        )

    def reader_get(self) -> dict:
        """
        Perform all the operation for ReaderMessageBroker
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
        reader_model = ReaderMessageBrokerModel()
        """
        Assign Model Class key's for caller
        """
        reader_model[NODE_INPUT_GEN_SPARK] = self.spark
        reader_model[NODE_INPUT_GEN_FORMAT] = self._reader_get_format()[NODE_INPUT_GEN_FORMAT]
        reader_model[NODE_INPUT_GEN_SCHEMA] = self._reader_get_schema()[NODE_INPUT_GEN_SCHEMA]
        reader_model[NODE_INPUT_GEN_OPTIONS] = self._reader_get_options()[NODE_INPUT_GEN_OPTIONS]

        return reader_model.get_all_data()

    def _reader_get_format(self) -> dict:
        """
        Get the Format for the Reader
        :return:
        """
        return {
            NODE_INPUT_GEN_FORMAT: self.broker_data_format
        }

    def _reader_get_schema(self) -> dict:
        """
        Get the Schema for the Reader Kafka Data Type
        :return:
        """
        spark_schema = None

        match self.broker_data_format:

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

    def _reader_get_options(self) -> dict:
        """
        Generate the Options for the Reader Message Broker Kafka
        :return:
        """
        return {
            NODE_INPUT_GEN_OPTIONS: self.__reader_set_options(),
        }

    def __reader_set_options(self):
        """
        Generate the Options for the Reader Message Broker Kafka
        :return:
        """
        options: dict = {
            NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_BOOTSTRAP_SERVER: self.broker_options[
                NODE_INPUT_FORMAT_OPTIONS_KAFKA_BOOTSTRAP_SERVER],
            NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SECURITY_PROTOCOL: self.broker_options[
                NODE_INPUT_FORMAT_OPTIONS_KAFKA_SECURITY_PROTOCOL],
            NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SOURCE_TOPIC: self.broker_options[
                NODE_INPUT_FORMAT_OPTIONS_KAFKA_SOURCE_TOPIC],
            NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SECRET_KEY_STORE_JKS: self.broker_options[NODE_INPUT_FORMAT_OPTIONS_KAFKA_SECRET_KEY_STORE_JKS],
            NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SECRET_KEY_STORE_PASSWORD: self.secret[
                self.broker_options[NODE_INPUT_FORMAT_OPTIONS_KAFKA_SECRET_KEY_STORE_PASSWORD]
            ],
            NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SECRET_TRUST_STORE_JKS: self.broker_options[NODE_INPUT_FORMAT_OPTIONS_KAFKA_SECRET_TRUST_STORE_JKS],
            NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SECRET_TRUST_STORE_PASSWORD: self.secret[
                self.broker_options[NODE_INPUT_FORMAT_OPTIONS_KAFKA_SECRET_TRUST_STORE_PASSWORD]
            ],
            NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SECRET_SSL_KEY_PASSWORD: self.secret[
                self.broker_options[NODE_INPUT_FORMAT_OPTIONS_KAFKA_SECRET_SSL_KEY_PASSWORD]
            ],
            NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SECRET_STARTING_OFFSET: self.broker_options[
                NODE_INPUT_FORMAT_OPTIONS_KAFKA_SECRET_STARTING_OFFSET],
            NODE_INPUT_FORMAT_GEN_OPTIONS_KAFKA_SECRET_FAIL_ON_DATA_LOSS: self.broker_options[
                NODE_INPUT_FORMAT_OPTIONS_KAFKA_SECRET_FAIL_ON_DATA_LOSS
            ]

        }
        return options

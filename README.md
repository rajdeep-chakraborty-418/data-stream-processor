# data-stream-processor
# Utility Module with "Bring Your Own Config" Feature

Utility Module provides a set of helpful functions and tools that can be easily extended with custom configurations. 
It allows users to bring their own business logic as part of config and execute using this module, 
making it flexible for different use cases without building code framework with Splunk support for Metric

The **"Bring Your Own Config"** (BYOC) feature enables users to inject their own business logic as configurations without 
modifying the module's core code. 

This approach offers high flexibility and extensibility while maintaining a clean and reusable codebase.

## Features

- **PySpark Support**: Built on PySpark, this module is designed to handle large-scale data processing tasks efficiently

- **Custom Configuration Support**: Easily pass your own configurations without changing the codebase

- **Extensible and Flexible**: Works across various projects, making it easy to configure the module to meet your specific needs

- **Simple Setup**: Configure and start using the module in a few simple steps.

- **Logging and Debugging Support**: Built-in logging for better debugging.

- **Security**: 
  - Kafka Passwords will be consumed from AWS Secret Manager
  - Kafka JKS Files will be read from Cluster Runtime locations (Some Init Script will push as part of Infra)

- **Splunk Support** - Built-in metric push to splunk for observability with user defined namespace & customised metric name
  - Supported Metric Type - 
    - Gauge
    - Counter
  - Supported Metrics
    - Time taken for each microbatch
    - Time taken for all transformations in each microbatch
    - Time taken for write to output in each microbatch
    - #Records written to output in each microbatch
    - #Records processed in each microbatch

- **Multiple Transformation Type Support**: 

  - Supports SQL-based transformations, allowing users to define custom transformation rules
  - Supports Pyspark-based transformations, enabling users to write custom PySpark code for complex data processing

## Environment Variables
    
- **CONFIG_FILE_PATH="Path Of Config File"**
- **REGION="AWS Region"**
- **CONFIG_READER_SOURCE="S3/databricks_volume"**
- **SPLUNK_HEC_URL="Splunk Observability Cloud URL"**
- **SPLUNK_HEC_TOKEN="Splunk Token"**

## Supported Configurations

- **File Formats**: Support for file formats
  - ***Reader Supported Formats**: (JSON, AVRO)
  - ***Writer Supported Formats**: (JSON, AVRO)
  
- **Kafka Formats**: Support for Kafka Broker
  - ***Kafka Reader Supported Data Formats**: (JSON, AVRO)
  - ***Kafka Writer Supported Formats**: (JSON)

### File Format Supported Configurations

```
File Format Configurations
{
  "aws_region": "",
  "environment": "",
  "logger_name": "",
  "processor_typ": "",
  "input": {
    "reader_typ": "file_system", 
    "sourcePath": "s3://....../", 
    "format": "json/avro",
    "filePattern": "pattern-*.json/pattern-*.avro",
    "format_options": {
      "maxFilesPerTrigger": 100",
      "multiLine": true
    },
    "schema": {
      "type": "struct",
      "fields": [
        {
          "name": "id",
          "type": "integer",
          "nullable": true,
          "metadata": {}
        },
        {
          "name": "user",
          "type": {
            "type": "struct",
            "fields": [
              {
                "name": "name",
                "type": "string",
                "nullable": true,
                "metadata": {}
              },
              {
                "name": "email",
                "type": "string",
                "nullable": true,
                "metadata": {}
              }
            ]
          },
          "nullable": true,
          "metadata": {}
        },
        {
          "name": "created_at",
          "type": "timestamp",
          "nullable": true,
          "metadata": {}
        }
      ]
    }
  },
  "transform": {
    "input": "raw_input",
    "conversion": [
      {
        "transform_typ": "sql",
        "transform_seq": "1",
        "transform_name": "flatten_input_fields",
        "transform_input": { 
          "config_input_table_1": "raw_input"
        },
        "transform_output": {
          "config_output_table":
        },
        "transform_query": "s3://....../", 
      }
    ],
    "output": "transform_output"
  },
  "output": {
    "writer_typ": "file_system",
    "targetPath": "s3://..../",
    "format": "json",
    "outputMode": "append",
    "stream_options": {
      "checkpointLocation": "s3://..../"
    },
    "partitionBy": [],
    "writer_typ_options": {
      "compression": "snappy"
    }
  },
  "metric" : {
    "namespace": "......",
    "metric_set": {
      "microbatch_time": {
        "typ": "gauge",
        "custom_metric_name": "......"
      },
      "microbatch_transformation_time": {
        "typ": "gauge",
        "custom_metric_name": "......"
      },
      "microbatch_writer_time": {
        "typ": "gauge",
        "custom_metric_name": "......"
      },
      "microbatch_writer_record_count": {
        "typ": "gauge",
        "custom_metric_name": "......"
      },
      "microbatch_record_count": {
        "typ": "counter",
        "custom_metric_name": "......"
      }
    },
    "flush_interval": ..,
    "flush_batch_size": ..
  }
}
```

### Sample Config

* **aws_region**: AWS region where the data is stored ("us-west-2")
* **environment**: Environment name ("dev", "prod")
* **logger_name**: Name of the logger to be used for logging
* **processor_typ**: Type of the processor ("streaming")
* **input**: Configuration for the input data source
  * **reader_typ**: Type of the reader ("file_system")
  * **sourcePath**: Path to the data source ("s3://bucket/path/")
  * **format**: Format of the input data ("json" / "avro")
  * **filePattern**: Pattern to match files in the source path
  * **format_options**: Additional options for the format
    * **maxFilesPerTrigger**: Maximum number of files to process per trigger
    * **multiLine**: Whether the input is multi-line (for JSON)
  * **schema**: Schema of the input data ( It should be compatible to JSON/AVRO)
* **transform**: Configuration for data transformations
  * **input**: Name of the input table to transform ("raw_input")
  * **conversion**: List of transformations to apply on source data
    * **transform_typ**: Type of transformation ("sql"/"pyspark")
    * **transform_seq**: Sequence number of the transformation for order of execution
    * **transform_name**: Name of the transformation
    * **transform_input**: Input configuration for the transformation rule
      * **config_input_table_1**: Name of the input table for the transformation
      * **config_input_table_2**: Name of the input table for the transformation
      * **config_input_table_<n>**: Name of the input table for the transformation
    * **transform_output**: Output configuration for the transformation
      * **config_output_table**: Name of the output table for the transformation
    * **transform_query**: Query or path to the query file ("s3://bucket/path/"). 
        For SQL and Pyspark please ensure that the query is compatible with the 
        transformation type and should take the input and output table names as parameters defined
        as keys in `transform_input` and `transform_output`
  * **output: Name of the output table after all transformations
* **output: Configuration for the output data sink  
  * **writer_typ**: Type of the writer ("file_system")
  * **targetPath**: Path to write the output data ("s3://bucket/path/")
  * **format**: Format of the output data ("json" / "avro")
  * **outputMode**: Mode for writing output data ("append")
  * **stream_options**: Options for streaming compatibility
    * **checkpointLocation**: Location for storing checkpoints
  * **partitionBy**: List of columns to partition the output data by. Optional
  * **writer_typ_options**: Additional options for the writer for the format
    * **compression**: Compression type for the output data ("snappy", "gzip")
* **metric: Splunk Metric Configuration for inbuilt metric publish
  * **namespace**: Namespace of the metrics in splunk observability cloud
  * **metric_set**: Set of Supported Metrics
    * **microbatch_time**: Time taken for each microbatch
      * **typ**: gauge (Immutable)
      * **custom_metric_name**: Custom mame of the metric in splunk cloud 
    * **microbatch_transformation_time**: Time taken for all transformations in each microbatch
        * **typ**: gauge (Immutable)
        * **custom_metric_name**: Custom mame of the metric in splunk cloud
    * **microbatch_writer_time**: Time taken for write to output in each microbatch
        * **typ**: gauge (Immutable)
        * **custom_metric_name**: Custom mame of the metric in splunk cloud
    * **microbatch_writer_record_count**: # Records written to output in each microbatch
        * **typ**: gauge (Immutable)
        * **custom_metric_name**: Custom mame of the metric in splunk cloud
    * **microbatch_record_count**: # Records processed in each microbatch
        * **typ**: gauge (Immutable)
        * **custom_metric_name**: Custom mame of the metric in splunk cloud
  * **flush_interval**: Splunk Metric Push Thread Flush interval
  * **flush_batch_size**: Splunk Metric Push Thread Batch Size

### Message Broker (Kafka) Supported Configurations

### Sample Config

```
Kafka Reader & Writer Supported Configurations
{
  "aws_region": "",
  "environment": "",
  "logger_name": "",
  "processor_typ": "",
  "input": {
    "reader_typ": "message_broker_kafka",
    "format": "json",
    "secret_name": "",
    "format_options": {
      "kafka.bootstrap.servers": "",
      "kafka.security.protocol": "SSL",
      "subscribe": "",
      "kafka.ssl.keystore.location": "",
      "kafka.ssl.keystore.password": "",
      "kafka.ssl.truststore.location": "",
      "kafka.ssl.truststore.password": "",
      "kafka.ssl.key.password": "",
      "startingOffsets": ""
    },
    "schema": {
      "type": "struct",
      "fields": [
        {
          "name": "id",
          "type": "integer",
          "nullable": true,
          "metadata": {}
        },
        {
          "name": "user",
          "type": {
            "type": "struct",
            "fields": [
              {
                "name": "name",
                "type": "string",
                "nullable": true,
                "metadata": {}
              },
              {
                "name": "email",
                "type": "string",
                "nullable": true,
                "metadata": {}
              }
            ]
          },
          "nullable": true,
          "metadata": {}
        },
        {
          "name": "created_at",
          "type": "timestamp",
          "nullable": true,
          "metadata": {}
        }
      ]
    }
  },
  "transform": {
    "input": "raw_input",
    "conversion": [
      {
        "transform_typ": "sql",
        "transform_seq": "1",
        "transform_name": "flatten_input_fields",
        "transform_input": { 
          "config_input_table_1": "raw_input"
        },
        "transform_output": {
          "config_output_table":
        },
        "transform_query": "s3://....../", 
      }
    ],
    "output": "transform_output"
  },
  "output": {
    "writer_typ": "file_system",
    "secret_name": "",
    "outputMode": "append",
    "stream_options": {
      "checkpointLocation": "s3://..../"
    },
    "writer_typ_options": {
      "kafka.bootstrap.servers": "",
      "kafka.security.protocol": "SSL",
      "topic": "",
      "kafka.ssl.keystore.location": "",
      "kafka.ssl.keystore.password": "",
      "kafka.ssl.truststore.location": "",
      "kafka.ssl.truststore.password": "",
      "kafka.ssl.key.password": "",
      "startingOffsets": "latest",
      "failOnDataLoss": "false"
    }
  },
  "metric" : {
    "namespace": "......",
    "metric_set": {
      "microbatch_time": {
        "typ": "gauge",
        "custom_metric_name": "......"
      },
      "microbatch_transformation_time": {
        "typ": "gauge",
        "custom_metric_name": "......"
      },
      "microbatch_writer_time": {
        "typ": "gauge",
        "custom_metric_name": "......"
      },
      "microbatch_writer_record_count": {
        "typ": "gauge",
        "custom_metric_name": "......"
      },
      "microbatch_record_count": {
        "typ": "counter",
        "custom_metric_name": "......"
      }
    },
    "flush_interval": ..,
    "flush_batch_size": ..
  }
}

```
### Config Description

* **aws_region**: AWS region where the data is stored ("us-west-2")
* **environment**: Environment name ("dev", "prod")
* **logger_name**: Name of the logger to be used for logging
* **processor_typ**: Type of the processor ("streaming")
* **input**: Configuration for the input data source
    * **reader_typ**: Type of the reader ("message_broker_kafka")
    * **format**: Format of the input data ("json" / "avro")
    * **secret_name**: AWS Secret Manager Secret Name stores the Kafka connection details
    * **format_options**: Additional options for the format
        * **kafka.bootstrap.servers**: Kafka Bootstrap Server
        * **kafka.security.protocol**: SSL
        * **subscribe**: Topic to subscribe to
        * **kafka.ssl.keystore.location**: Location of the Kafka SSL keystore jks in local cluster path (eg: /var/.jks/)
        * **kafka.ssl.keystore.password**: Password for the Kafka SSL keystore retrieved from secret
        * **kafka.ssl.truststore.location**: Location of the Kafka SSL truststore jks (eg: /var/.jks/)
        * **kafka.ssl.truststore.password**: Password for the Kafka SSL truststore retrieved from secret
        * **kafka.ssl.key.password**: Password for the Kafka SSL key retrieved from secret
        * **startingOffsets**: Starting offsets for the Kafka topic (e.g., "earliest", "latest")
        * **failOnDataLoss**: Stops streaming from failure from data non availability in Kafka
    * **schema**: Schema of the input data ( It should be compatible to JSON/AVRO)
* **transform**: Configuration for data transformations
    * **input**: Name of the input table to transform ("raw_input")
    * **conversion**: List of transformations to apply on source data
        * **transform_typ**: Type of transformation ("sql"/"pyspark")
        * **transform_seq**: Sequence number of the transformation for order of execution
        * **transform_name**: Name of the transformation
        * **transform_input**: Input configuration for the transformation rule
            * **config_input_table_1**: Name of the input table for the transformation
            * **config_input_table_2**: Name of the input table for the transformation
            * **config_input_table_<n>**: Name of the input table for the transformation
        * **transform_output**: Output configuration for the transformation
            * **config_output_table**: Name of the output table for the transformation
        * **transform_query**: Query or path to the query file ("s3://bucket/path/").
          For SQL and Pyspark please ensure that the query is compatible with the
          transformation type and should take the input and output table names as parameters defined
          as keys in `transform_input` and `transform_output`
    * **output: Name of the output table after all transformations
* **output: Configuration for the output Kafka sink
    * **writer_typ**: Type of the writer ("message_broker_kafka")
    * **outputMode**: Mode for writing output data ("append")
    * **stream_options**: Options for streaming compatibility
        * **checkpointLocation**: Location for storing checkpoints
    * **writer_typ_options**: Options for the Kafka Writer
        * **kafka.bootstrap.servers**: Kafka Bootstrap Server
        * **kafka.security.protocol**: SSL
        * **topic**: Topic to write to
        * **kafka.ssl.keystore.location**: Location of the Kafka SSL keystore jks
        * **kafka.ssl.keystore.password**: Password for the Kafka SSL keystore retrieved from secret
        * **kafka.ssl.truststore.location**: Location of the Kafka SSL truststore jks
        * **kafka.ssl.truststore.password**: Password for the Kafka SSL truststore retrieved from secret
        * **kafka.ssl.key.password**: Password for the Kafka SSL key retrieved from secret
* **metric: Splunk Metric Configuration for inbuilt metric publish
    * **namespace**: Namespace of the metrics in splunk observability cloud
    * **metric_set**: Set of Supported Metrics
        * **microbatch_time**: Time taken for each microbatch
            * **typ**: gauge (Immutable)
            * **custom_metric_name**: Custom mame of the metric in splunk cloud
        * **microbatch_transformation_time**: Time taken for all transformations in each microbatch
            * **typ**: gauge (Immutable)
            * **custom_metric_name**: Custom mame of the metric in splunk cloud
        * **microbatch_writer_time**: Time taken for write to output in each microbatch
            * **typ**: gauge (Immutable)
            * **custom_metric_name**: Custom mame of the metric in splunk cloud
        * **microbatch_writer_record_count**: # Records written to output in each microbatch
            * **typ**: gauge (Immutable)
            * **custom_metric_name**: Custom mame of the metric in splunk cloud
        * **microbatch_record_count**: # Records processed in each microbatch
            * **typ**: gauge (Immutable)
            * **custom_metric_name**: Custom mame of the metric in splunk cloud
  * **flush_interval**: Splunk Metric Push Thread Flush interval
  * **flush_batch_size**: Splunk Metric Push Thread Batch Size

### Spark SQL Transformations

* **Bring your own Spark SQL based transformations** in .sql files
* Should be valid Spark SQL statements
* Table Names referred in the queries should be based on transform_input node
    * table names should be referred in queries like {config_input_table_1}
    * During runtime, it would be substituted by actual table name defined in transform_input.config_input_table_1 ... transform_input.config_input_table_n
    * An SQL query can support as many tables based on previous step and input data given they have been created in previous steps
    * Refer to below set of pyspark transformations as sample to understand
      * mock/kafka_processor/spark_sql/1_cleanse_normalise_log_events.sql
      * mock/kafka_processor/spark_sql/2_transform_log_events.sql

### PySpark Transformations

* **Bring your own Pyspark based transformations** in .py files
* Should be valid Pyspark statements
* Table Names referred in the queries should match with transform_input node
    * Each transform script should have a method called def transform(spark) which will be executed in runtime
    * The method will take a SparkSession object as input. Same will be passed from the interface
    * Input table names defined the config should be used as dataframes inside script to write the business logic
    * A Pyspark can support as many tables based on previous step and input data given they have been created in previous steps
    * framework will convert config input tables into spark dataframe
    * Refer to below set of pyspark transformations as sample to understand
      * mock/kafka_processor/pyspark/1_cleanse_normalise_log_events.py
      * mock/kafka_processor/pyspark/2_transform_log_events.py

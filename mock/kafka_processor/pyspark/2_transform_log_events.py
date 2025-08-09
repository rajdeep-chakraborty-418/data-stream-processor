from pyspark.sql.functions import (
    col,
    when,
    lit,
    current_timestamp,
    array,
    concat,
    coalesce,
    array_remove,
    concat_ws,
    to_json,
    struct,
)

"""
Generate Error Message Format For Failed Validations for reporting
"""
validation_fields = ["timestamp", "src_ip", "dest_ip", "action", "protocol"]
validation_cols = [f"""valid_{f}""" for f in validation_fields]
error_fields_expr = array(
    *[
        when(
            ~col(valid_col), concat(
                lit(f"""{field}="""),
                coalesce(
                    col(field).cast("string"), lit("NULL")
                )
            )
        )
        .otherwise(lit("XX"))
        for field, valid_col in zip(validation_fields, validation_cols)
    ]
)


def transform(spark):
    """
    Transform function for cleansing and normalizing the input DataFrame.
    This function is expected to be called by the ScriptExecutor with a valid Spark session
    """
    """
    Retrieve the input DataFrame from the Spark session
    """
    input_dataframe = cleansed_normalised_output
    """
    Generate Valid / Invalid Record Based on Cleansing / Normalisation
    """
    input_dataframe = input_dataframe.withColumn(
        "valid_record",
        col("valid_src_ip") &
        col("valid_dest_ip") &
        col("valid_action") &
        col("valid_protocol") &
        col("valid_timestamp") &
        col("valid_bytes")
    ).withColumn(
        "bytes_category",
        when(
            col("bytes_int") < 1000,
            lit("small")
        )
        .when(
            (col("bytes_int") >= 1000) & (col("bytes_int") < 5000),
            lit("medium")
        )
        .otherwise(
            lit("large")
        )
    ).withColumn(
        "processed_at",
        current_timestamp()
    ).withColumn(
        "event_status",
        when(
            col("valid_record"), lit("valid")
        ).otherwise(
            lit("invalid")
        )
    )

    input_dataframe = input_dataframe.select(
        "timestamp",
        "src_ip",
        "dest_ip",
        "action",
        "protocol",
        col("bytes_int").alias("bytes"),
        "bytes_category",
        "processed_at",
        "event_status",
        "valid_src_ip",
        "valid_dest_ip",
        "valid_action",
        "valid_protocol",
        "valid_timestamp",
        "valid_bytes"
    )
    """
    Separate Valid & Invalid Dataframe
    """
    valid_df = input_dataframe.filter(col("event_status") == "valid")
    invalid_df = input_dataframe.filter(col("event_status") == "invalid")

    invalid_df = invalid_df.withColumn(
        "error_fields", error_fields_expr
    ).withColumn(
        "error_fields", array_remove(
            col("error_fields"), "XX"
        )
    ).withColumn(
        "error_message",
        when(
            col("error_fields").isNotNull() & (col("error_fields").getItem(0).isNotNull()),
            concat_ws(
                ", ", col("error_fields")
            )
        ).otherwise(lit(None))
    )
    invalid_df = invalid_df.select(
        "timestamp",
        "src_ip",
        "dest_ip",
        "action",
        "protocol",
        "bytes",
        "bytes_category",
        "processed_at",
        "event_status",
        "error_message"
    )
    final_df = valid_df.select(
        "timestamp",
        "src_ip",
        "dest_ip",
        "action",
        "protocol",
        "bytes",
        "bytes_category",
        "processed_at",
        "event_status",
        lit("").alias("error_message"),
    ).union(invalid_df)

    final_df = final_df.select(
        col("event_status").alias("key"),
        to_json(
            struct(
                "timestamp",
                "src_ip",
                "dest_ip",
                "action",
                "protocol",
                "bytes",
                "bytes_category",
                "processed_at",
                "event_status",
                "error_message"
            )
        ).alias("value")
    )
    final_df = final_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    return final_df

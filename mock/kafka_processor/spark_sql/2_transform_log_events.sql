WITH validation_output AS (
    SELECT
        x.timestamp,
        x.src_ip,
        x.dest_ip,
        x.action,
        x.protocol,
        x.bytes_int,
        x.valid_src_ip,
        x.valid_dest_ip,
        x.valid_action,
        x.valid_protocol,
        x.valid_timestamp,
        x.valid_bytes,
        x.valid_record
    FROM {config_input_table_1} x
),
transformation AS (
    SELECT
        x.*,
        CASE
            WHEN x.bytes_int < 1000 THEN 'small'
            WHEN x.bytes_int >= 1000 AND x.bytes_int < 5000 THEN 'medium'
            ELSE 'large'
        END AS bytes_category,
        current_timestamp() AS processed_at,
        CASE
            WHEN x.valid_record THEN 'valid'
            ELSE 'invalid'
        END AS event_status,
        x.bytes_int AS bytes
    FROM validation_output x
),
valid_events AS (
    SELECT
        x.timestamp,
        x.src_ip,
        x.dest_ip,
        x.action,
        x.protocol,
        x.bytes,
        x.bytes_category,
        x.processed_at,
        x.event_status,
        '' AS error_message,
        x.valid_src_ip,
        x.valid_dest_ip,
        x.valid_action,
        x.valid_protocol,
        x.valid_timestamp,
        x.valid_bytes
    FROM transformation x
    WHERE x.event_status = 'valid'
),
invalid_events_with_errors AS (
    SELECT
        timestamp,
        src_ip,
        dest_ip,
        action,
        protocol,
        bytes,
        bytes_category,
        processed_at,
        event_status,
        filter(
            array(
                CASE
                    WHEN NOT y.valid_timestamp THEN concat('timestamp=', coalesce(CAST(y.timestamp AS STRING), 'NULL'))
                    ELSE NULL
                END,
                CASE
                    WHEN NOT y.valid_src_ip THEN concat('src_ip=', coalesce(y.src_ip, 'NULL'))
                    ELSE NULL
                END,
                CASE
                    WHEN NOT y.valid_dest_ip THEN concat('dest_ip=', coalesce(y.dest_ip, 'NULL'))
                    ELSE NULL
                END,
                CASE
                    WHEN NOT y.valid_action THEN concat('action=', coalesce(y.action, 'NULL'))
                    ELSE NULL
                END,
                CASE
                    WHEN NOT y.valid_protocol THEN concat('protocol=', coalesce(y.protocol, 'NULL'))
                    ELSE NULL
                END
            ),
            x -> x IS NOT NULL
        ) AS error_fields
    FROM transformation y
    WHERE y.event_status = 'invalid'
),
invalid_data AS (
    SELECT
        x.timestamp,
        x.src_ip,
        x.dest_ip,
        x.action,
        x.protocol,
        x.bytes,
        x.bytes_category,
        x.processed_at,
        x.event_status,
        CASE
            WHEN size(x.error_fields) > 0 THEN concat_ws(', ', x.error_fields)
            ELSE NULL
        END AS error_message
    FROM invalid_events_with_errors x
),
final_combined_events AS (
    SELECT
        x.timestamp,
        x.src_ip,
        x.dest_ip,
        x.action,
        x.protocol,
        x.bytes,
        x.bytes_category,
        x.processed_at,
        x.event_status,
        x.error_message
    FROM valid_events x

    UNION ALL

    SELECT
        x.timestamp,
        x.src_ip,
        x.dest_ip,
        x.action,
        x.protocol,
        x.bytes,
        x.bytes_category,
        x.processed_at,
        x.event_status,
        x.error_message
    FROM invalid_data x
),
final_convert_key_value AS (
    SELECT
        x.event_status AS key,
        TO_JSON(
            STRUCT(
                x.timestamp,
                x.src_ip,
                x.dest_ip,
                x.action,
                x.protocol,
                x.bytes,
                x.bytes_category,
                x.processed_at,
                x.event_status,
                x.error_message
            )
        ) AS value
    FROM final_combined_events x
)

SELECT
    CAST(x.key AS STRING)   AS key,
    CAST(x.value AS STRING) AS value
FROM final_convert_key_value x
WITH trimmed_data AS (
    SELECT
        TRIM(x.timestamp) AS timestamp_str,
        TRIM(x.src_ip) AS src_ip,
        TRIM(x.dest_ip) AS dest_ip,
        x.bytes,
        TRIM(x.action) AS action,
    TRIM(x.protocol) AS protocol
    FROM {config_input_table_1} x
),
parsed_data AS (
    SELECT
        x.timestamp_str,
        x.src_ip,
        x.dest_ip,
        x.bytes,
        x.action,
        x.protocol,
        TO_TIMESTAMP(x.timestamp_str, "yyyy-MM-dd'T'HH:mm:ssX") AS timestamp,
        CAST(bytes AS INT) AS bytes_int
    FROM trimmed_data x
),
valid_ip AS (
    SELECT
        x.*,
        CASE
            WHEN x.src_ip RLIKE '^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$'
                AND CAST(split(x.src_ip, '\\.')[0] AS INT) BETWEEN 0 AND 255
                AND CAST(split(x.src_ip, '\\.')[1] AS INT) BETWEEN 0 AND 255
                AND CAST(split(x.src_ip, '\\.')[2] AS INT) BETWEEN 0 AND 255
                AND CAST(split(x.src_ip, '\\.')[3] AS INT) BETWEEN 0 AND 255
            THEN TRUE
            ELSE FALSE
        END AS valid_src_ip,
        CASE
            WHEN x.dest_ip RLIKE '^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$'
                AND CAST(split(x.dest_ip, '\\.')[0] AS INT) BETWEEN 0 AND 255
                AND CAST(split(x.dest_ip, '\\.')[1] AS INT) BETWEEN 0 AND 255
                AND CAST(split(x.dest_ip, '\\.')[2] AS INT) BETWEEN 0 AND 255
                AND CAST(split(x.dest_ip, '\\.')[3] AS INT) BETWEEN 0 AND 255
            THEN TRUE
            ELSE FALSE
        END AS valid_dest_ip
    FROM parsed_data x
),
validations AS (
    SELECT
        x.*,
        x.action IN ('ALLOW', 'DENY') AS valid_action,
        x.protocol IN ('TCP', 'UDP', 'ICMP') AS valid_protocol,
        x.timestamp IS NOT NULL AS valid_timestamp,
        x.bytes_int IS NOT NULL AS valid_bytes
    FROM valid_ip x
),
final AS (
    SELECT
        x.*,
        x.valid_src_ip
        AND x.valid_dest_ip
        AND x.valid_action
        AND x.valid_protocol
        AND x.valid_timestamp
        AND x.valid_bytes AS valid_record
    FROM validations x
)
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
FROM final x

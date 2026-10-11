-- Values Trino types as timestamp(3) with time zone are UTC and print with the zone; they compare with zone-less timestamps.
SELECT from_iso8601_timestamp('2024-01-05T10:00:00Z') AS iso_z,
       CAST(from_iso8601_timestamp('2024-01-05T10:00:00+00:00') AS VARCHAR) AS iso_text,
       hour(from_iso8601_timestamp('2024-01-05T10:00:00')) AS iso_hour,
       parse_datetime('2024-01-05 10:00:00', 'yyyy-MM-dd HH:mm:ss') AS parsed,
       CAST(parse_datetime('2024-01-05 10:00:00', 'yyyy-MM-dd HH:mm:ss') AS VARCHAR) AS parsed_text,
       CAST(parse_datetime('2024-01-05 10:00:00', 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) AS parsed_as_timestamp,
       from_iso8601_timestamp('2024-01-05T10:00:00Z') = TIMESTAMP '2024-01-05 10:00:00' AS equals_plain,
       from_iso8601_timestamp('2024-01-05T10:00:00Z') > created_at AS after_order,
       date_diff('minute', created_at, from_iso8601_timestamp('2024-01-05T11:00:00Z')) AS minutes_until,
       date_trunc('hour', from_iso8601_timestamp('2024-01-05T10:45:00Z')) AS truncated_keeps_zone,
       CAST(current_timestamp AS VARCHAR) LIKE '____-__-__ __:__:__.___ UTC' AS now_text_shape,
       current_timestamp > TIMESTAMP '2024-01-01 00:00:00' AS now_is_after_2024,
       date_parse('2024-01-05 10:00:00.9999', '%Y-%m-%d %H:%i:%s.%f') AS date_parse_truncates_to_millis
FROM orders
WHERE id = 101

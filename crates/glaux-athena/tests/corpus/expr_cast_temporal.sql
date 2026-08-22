-- varchar → TIMESTAMP / DATE follow Trino's patterns: fractions round HALF_UP to milliseconds, a date-only text is midnight, a DATE text must be exactly a calendar date.
SELECT CAST('2024-01-05 10:00:00.9999' AS TIMESTAMP) AS rounds_up_to_next_second,
       CAST('2024-01-05 10:00:00.1235' AS TIMESTAMP) AS rounds_half_up,
       CAST('2024-01-05 10:00:00.1234' AS TIMESTAMP) AS rounds_down,
       CAST('2024-01-05' AS TIMESTAMP) AS date_only_text,
       CAST('2024-1-5 1:2' AS TIMESTAMP) AS single_digit_fields,
       CAST(' 2024-01-05 10:00 ' AS TIMESTAMP) AS trimmed,
       CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS far_future,
       CAST('1600-01-01 00:00:00' AS TIMESTAMP) AS far_past,
       CAST(DATE '2024-01-05' AS TIMESTAMP) AS date_to_timestamp,
       CAST(TIMESTAMP '2024-01-05 10:30:00' AS DATE) AS timestamp_to_date,
       CAST(created_at AS DATE) AS column_to_date,
       CAST('2024-01-05' AS DATE) AS date_text,
       CAST(' 2024-1-5 ' AS DATE) AS short_date_text,
       TRY_CAST('2024-01-05 10:00:00+05:00' AS TIMESTAMP) AS try_zoned_is_null,
       TRY_CAST('2024-01-05 10:00:00' AS DATE) AS try_date_with_time_is_null,
       TRY_CAST('nope' AS TIMESTAMP) AS try_garbage_is_null,
       date('2024-02-29') AS date_function
FROM orders
WHERE id = 101

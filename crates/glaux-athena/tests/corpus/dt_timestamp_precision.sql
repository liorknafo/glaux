-- A nanosecond-precision column (Glue timestamp / Parquet micros) is rounded HALF_UP to milliseconds before any function sees it, as Athena reads timestamp(3).
SELECT id, at, second(at) AS sec, CAST(at AS VARCHAR) AS text, date_format(at, '%Y-%m-%d %H:%i:%s.%f') AS formatted,
       at = TIMESTAMP '2024-01-05 10:00:01' AS rounded_equals_literal,
       date_trunc('second', at) AS truncated, date_add('millisecond', 1, at) AS plus_ms,
       CAST(TIMESTAMP '2024-01-05 10:00:00.123' AS VARCHAR) AS literal_text
FROM events
ORDER BY id

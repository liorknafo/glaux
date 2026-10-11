-- from_unixtime is a timestamp(3) with time zone at UTC, like now() and parse_datetime.
SELECT from_unixtime(0) AS epoch, CAST(from_unixtime(0) AS VARCHAR) AS epoch_text,
       date_trunc('day', from_unixtime(1704448800)) AS truncated, from_unixtime(1.9999) AS rounded,
       hour(from_unixtime(1704448800)) AS h, from_unixtime(1704448800) > TIMESTAMP '2024-01-05 00:00:00' AS after_midnight

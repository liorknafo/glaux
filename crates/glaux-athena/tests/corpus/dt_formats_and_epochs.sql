-- Format-string translation (MySQL and Joda), ISO-8601 parsing, and unix epoch conversions.
SELECT id,
       date_format(created_at, '%Y-%m-%d %H:%i:%s') AS mysql_fmt,
       date_format(created_at, '%d/%b/%Y %l:%i %p') AS mysql_fmt2,
       format_datetime(created_at, 'yyyy-MM-dd''T''HH:mm:ss') AS joda_fmt,
       format_datetime(created_at, 'EEE, d MMM yyyy') AS joda_fmt2,
       date_parse('2024-03-15 12:00:00', '%Y-%m-%d %H:%i:%s') AS parsed_mysql,
       date_parse(CASE WHEN id = 105 THEN note END, '%Y-%m-%d %H:%i:%s') AS parsed_note,
       parse_datetime('15/03/2024 09:05', 'dd/MM/yyyy HH:mm') AS parsed_joda,
       from_iso8601_timestamp('2024-01-01T10:00:00Z') AS iso_ts,
       from_iso8601_timestamp('2024-01-01T10:00:00+00:00') AS iso_ts_offset,
       from_iso8601_date('2024-02-29') AS iso_date,
       from_unixtime(1700000000) AS epoch_int,
       from_unixtime(1700000000.5) AS epoch_frac,
       from_unixtime(1.9999) AS epoch_rounded,
       to_unixtime(created_at) AS as_epoch,
       to_unixtime(TIMESTAMP '2024-01-01 00:00:00.250') AS epoch_with_frac,
       current_date >= DATE '2024-01-01' AS today_is_after_2024,
       now() > TIMESTAMP '2024-01-01 00:00:00' AS now_is_after_2024,
       current_timestamp > TIMESTAMP '2024-01-01 00:00:00' AS cts_is_after_2024,
       localtimestamp > TIMESTAMP '2024-01-01 00:00:00' AS lts_is_after_2024
FROM orders
WHERE id IN (101, 105)
ORDER BY id

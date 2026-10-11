-- Joda numeric pattern letters print at least as many digits as the letter count: 'D' / 'w' / 'd' / 'H' are minimal, 'DDD' / 'dd' are padded.
SELECT format_datetime(TIMESTAMP '2024-01-05 14:05:09.123', 'D') AS doy_min,
       format_datetime(TIMESTAMP '2024-01-05 14:05:09.123', 'DDD') AS doy_padded,
       format_datetime(TIMESTAMP '2024-01-05 14:05:09.123', 'w') AS week_min,
       format_datetime(TIMESTAMP '2024-01-05 14:05:09.123', 'ww') AS week_padded,
       format_datetime(TIMESTAMP '2024-01-05 14:05:09.123', 'd/M/y H:m:s') AS all_minimal,
       format_datetime(TIMESTAMP '2024-01-05 14:05:09.123', 'dd/MM/yyyy HH:mm:ss') AS all_padded,
       format_datetime(TIMESTAMP '2024-12-30 14:05:09.123', 'xxxx-ww') AS iso_week_year

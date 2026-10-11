-- error: timestamp with time zone
SELECT parse_datetime('2024-01-05 10:00:00 +0200', 'yyyy-MM-dd HH:mm:ss Z')

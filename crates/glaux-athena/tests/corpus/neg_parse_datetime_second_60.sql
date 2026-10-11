-- error: Value 60 for secondOfMinute must be in the range [0,59]
SELECT parse_datetime('2024-01-05 10:30:60', 'yyyy-MM-dd HH:mm:ss')

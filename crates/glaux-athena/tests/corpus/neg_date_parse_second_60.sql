-- error: Value 60 for secondOfMinute must be in the range [0,59]
SELECT date_parse('2024-01-05 10:30:60', '%Y-%m-%d %H:%i:%s')

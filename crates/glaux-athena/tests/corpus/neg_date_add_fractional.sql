-- error: value must be an integer
SELECT date_add('day', 1.5, TIMESTAMP '2024-01-01 00:00:00')

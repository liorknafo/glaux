-- error: Offset must be at least 1
SELECT nth_value(x, 0) OVER (ORDER BY x) FROM (VALUES (1), (2)) t(x)

-- error: Offset must be at least 0
SELECT x, lag(x, -1) OVER (ORDER BY x) FROM (VALUES (1), (2)) t(x)

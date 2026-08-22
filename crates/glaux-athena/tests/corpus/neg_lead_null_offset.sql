-- error: Offset must not be null
SELECT x, lead(x, NULL) OVER (ORDER BY x) FROM (VALUES (1), (2)) t(x)

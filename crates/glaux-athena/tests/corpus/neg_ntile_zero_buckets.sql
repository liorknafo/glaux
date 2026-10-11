-- error: Buckets must be at least 1
SELECT ntile(0) OVER (ORDER BY x) FROM (VALUES (1), (2)) t(x)

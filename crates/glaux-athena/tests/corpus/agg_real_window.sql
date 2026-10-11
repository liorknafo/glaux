-- sum / avg over REAL as window functions keep the REAL result type.
SELECT x, sum(x) OVER () AS sum_all, avg(x) OVER (ORDER BY x) AS running_avg
FROM (VALUES (CAST(1.1 AS REAL)), (CAST(2.2 AS REAL))) t(x)
ORDER BY x

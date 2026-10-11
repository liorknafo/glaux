-- sum / avg over decimals: avg keeps the input scale with HALF_UP rounding (Trino), sum is decimal(38, s).
SELECT avg(x) AS avg_exact, sum(x) AS sum_exact, avg(DISTINCT x) AS avg_distinct, sum(DISTINCT x) AS sum_distinct, count(x) AS n,
       sum(x) / count(*) AS sum_over_count, avg(x) + 0.005 AS avg_plus
FROM (VALUES (CAST(1.50 AS DECIMAL(10,2))), (CAST(2.51 AS DECIMAL(10,2))), (CAST(2.51 AS DECIMAL(10,2))), (NULL)) t(x)

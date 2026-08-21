-- sum / avg over REAL return REAL (double accumulator cast to float), as Trino's RealSumAggregation / RealAverageAggregation.
SELECT avg(x) AS avg_real, sum(x) AS sum_real, max(x) AS max_real, sum(DISTINCT x) AS sum_distinct, avg(DISTINCT x) AS avg_distinct, count(x) AS n
FROM (VALUES (CAST(1.1 AS REAL)), (CAST(2.2 AS REAL)), (CAST(2.2 AS REAL)), (CAST(NULL AS REAL))) t(x)

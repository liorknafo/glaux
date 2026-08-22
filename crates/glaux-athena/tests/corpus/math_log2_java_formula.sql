-- Trino's MathFunctions.log2 is `Math.log(num) / Math.log(2)`, not a
-- base-2 logarithm routine; DataFusion's `log2` intrinsic is an ULP off
-- for inputs such as 3 and 100.
SELECT
  log2(CAST(3 AS DOUBLE)) AS l3,
  log2(CAST(100 AS DOUBLE)) AS l100,
  log2(CAST(8 AS DOUBLE)) AS l8,
  log2(CAST(3 AS DOUBLE)) = log(CAST(2 AS DOUBLE), CAST(3 AS DOUBLE)) AS agrees_with_log

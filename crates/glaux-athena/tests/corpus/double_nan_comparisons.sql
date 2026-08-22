-- Trino compares doubles with Java's primitive operators (DoubleType):
-- every comparison involving NaN is false and NaN <> NaN is true;
-- Arrow's total-order kernels would say NaN = NaN.
SELECT f,
       f = f AS self_eq,
       f = CAST('NaN' AS DOUBLE) AS eq_nan,
       f <> CAST('NaN' AS DOUBLE) AS neq_nan,
       1e0 < CAST('NaN' AS DOUBLE) AS lt_nan,
       1e0 <= CAST('NaN' AS DOUBLE) AS lte_nan,
       1e0 > CAST('NaN' AS DOUBLE) AS gt_nan,
       f IN (CAST('NaN' AS DOUBLE), 2e0) AS in_nan_list,
       f BETWEEN 0e0 AND 2e0 AS in_range,
       CASE f WHEN CAST('NaN' AS DOUBLE) THEN 'nan' ELSE 'other' END AS simple_case
FROM (VALUES (CAST('NaN' AS DOUBLE)), (1e0), (2e0)) t(f)

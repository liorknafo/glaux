-- contains / array_position / array_remove use Trino's EQUAL operator:
-- NaN equals nothing, -0.0 equals 0.0. array_distinct groups NaNs
-- (IDENTICAL semantics), so one NaN survives.
SELECT contains(ARRAY[CAST('NaN' AS DOUBLE)], CAST('NaN' AS DOUBLE)) AS c_nan,
       contains(ARRAY[0e0], -0e0) AS c_zero,
       array_position(ARRAY[CAST('NaN' AS DOUBLE), 1e0], CAST('NaN' AS DOUBLE)) AS p_nan,
       array_position(ARRAY[1e0, 2e0], 2e0) AS p_found,
       array_position(ARRAY[1e0], 3e0) AS p_missing,
       array_remove(ARRAY[CAST('NaN' AS DOUBLE), 1e0], CAST('NaN' AS DOUBLE)) AS r_nan,
       array_remove(ARRAY[1e0, 2e0, 1e0], 1e0) AS r_all,
       array_distinct(ARRAY[CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE)]) AS d_nan

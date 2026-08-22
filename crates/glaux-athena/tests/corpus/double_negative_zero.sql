-- nullif uses the EQUAL operator: 0.0 equals -0.0 (NULL), NaN never
-- equals itself (the value comes back).
SELECT f,
       nullif(f, 0e0) AS nf,
       nullif(0e0, -0e0) AS zero_zero,
       nullif(CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE)) AS nan_nan,
       0e0 = -0e0 AS zeros_equal
FROM (VALUES (-0e0), (1e0)) t(f)

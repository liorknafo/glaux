-- CAST(varchar AS DOUBLE / REAL) follows Java's Double.parseDouble.
SELECT CAST('NaN' AS DOUBLE) AS nan, CAST('Infinity' AS DOUBLE) AS inf, CAST('-Infinity' AS DOUBLE) AS neg_inf, CAST('+Infinity' AS REAL) AS pos_inf_real,
       CAST(' 1.5 ' AS DOUBLE) AS trimmed, CAST('1.5d' AS DOUBLE) AS d_suffix, CAST('2f' AS REAL) AS f_suffix, CAST('.5' AS DOUBLE) AS leading_point,
       CAST('1.' AS DOUBLE) AS trailing_point, CAST('-2e3' AS DOUBLE) AS exponent, CAST('1e40' AS REAL) AS real_overflow,
       TRY_CAST('nan' AS DOUBLE) AS try_nan, TRY_CAST('inf' AS DOUBLE) AS try_inf, TRY_CAST('infinity' AS REAL) AS try_infinity, TRY_CAST('0x1p3' AS DOUBLE) AS try_hex,
       CAST(1 AS DOUBLE) AS from_int, CAST(2.5 AS REAL) AS from_decimal, CAST(true AS DOUBLE) AS from_boolean, CAST(CAST(1.1 AS REAL) AS DOUBLE) AS real_to_double

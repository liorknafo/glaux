-- varchar → boolean accepts only true/false/t/f/1/0 (any case); numbers are non-zero tests.
SELECT CAST('t' AS BOOLEAN) AS t, CAST('0' AS BOOLEAN) AS zero, CAST('FALSE' AS BOOLEAN) AS upper_false, CAST('True' AS BOOLEAN) AS mixed_true,
       TRY_CAST('on' AS BOOLEAN) AS try_on_null, TRY_CAST('yes' AS BOOLEAN) AS try_yes_null,
       CAST(1 AS BOOLEAN) AS int_one, CAST(0 AS BOOLEAN) AS int_zero, CAST(2.5e0 AS BOOLEAN) AS double_nonzero, CAST(0.0 AS BOOLEAN) AS dec_zero,
       CAST(0.1 AS BOOLEAN) AS dec_nonzero, CAST(true AS BOOLEAN) AS identity, CAST(CAST('t' AS BOOLEAN) AS VARCHAR) AS round_trip, CAST(rush AS VARCHAR) AS column_text
FROM orders
WHERE id = 101

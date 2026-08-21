-- CAST(... AS DECIMAL(p, s)): doubles through their exact binary expansion (HALF_UP), decimals rescaled HALF_UP, varchar parsed exactly, overflow loud.
SELECT CAST(1e0 AS DECIMAL(38,37)) AS one_exact,
       CAST(1e0 / 3 AS DECIMAL(38,20)) AS third_exact,
       CAST(2.5e0 AS DECIMAL(2,0)) AS half_up, CAST(-2.5e0 AS DECIMAL(2,0)) AS half_up_neg,
       CAST(0.125e0 AS DECIMAL(3,2)) AS eighth, CAST(1.25 AS DECIMAL(2,1)) AS dec_rescaled,
       CAST('1.25' AS DECIMAL(3,1)) AS text_rescaled, CAST(' 1.5e-1 ' AS DECIMAL(5,3)) AS text_exponent,
       CAST(12 AS DECIMAL(4,1)) AS int_widened, CAST(amount AS DECIMAL(18,4)) AS column_cast,
       CAST(true AS DECIMAL(3,1)) AS bool_one, CAST(1.5 AS DECIMAL) AS default_precision,
       TRY_CAST(1e3 AS DECIMAL(2,0)) AS try_overflow_null, TRY_CAST('abc' AS DECIMAL(5,2)) AS try_text_null
FROM orders
WHERE id = 101

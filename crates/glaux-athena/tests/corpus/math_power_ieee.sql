-- Trino's `power` is Java's `Math.pow`: IEEE 754 pow, so a zero base with a
-- negative exponent is Infinity. DataFusion 55 keeps PostgreSQL's guard
-- ("zero raised to a negative power is undefined"), which is data-dependent
-- and killed whole queries as soon as one row held a zero.
SELECT power(0, -1) AS zero_neg,
       power(CAST(0 AS DOUBLE), CAST(-1 AS DOUBLE)) AS zero_neg_double,
       power(0.0, -2) AS zero_neg_decimal,
       power(-0e0, -1) AS signed_zero,
       power(amount, -1) AS from_column,
       power(amount - amount, -1) AS zero_from_column,
       power(2, 10) AS ordinary,
       power(2, 0.5) AS fractional,
       pow(-8, 1.0 / 3.0) AS negative_base_non_integer,
       -- The four cases where Math.pow departs from C's pow.
       power(1, CAST('NaN' AS DOUBLE)) AS one_to_nan,
       power(CAST('NaN' AS DOUBLE), 0) AS nan_to_zero,
       power(1, CAST('Infinity' AS DOUBLE)) AS one_to_inf,
       power(-1, CAST('Infinity' AS DOUBLE)) AS minus_one_to_inf
FROM orders
WHERE id = 101

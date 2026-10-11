-- Trino's common supertype of a double (or a real) and an exact number is a
-- double, in ARRAY[...] as in coalesce / CASE / UNION. DataFusion alone
-- unifies the elements as decimal(38,15) and prints `1.000000000000000`.
SELECT CAST(ARRAY[CAST(1 AS DOUBLE), CAST(1 AS DECIMAL(2,1))][1] AS VARCHAR) AS double_decimal,
       array_join(ARRAY[CAST(1 AS DOUBLE), CAST(1 AS DECIMAL(2,1))], ',') AS joined,
       CAST(ARRAY[CAST(1 AS REAL), CAST(1 AS DECIMAL(2,1))][1] AS VARCHAR) AS real_decimal,
       array_join(ARRAY[CAST(1 AS DOUBLE), 1.5], ',') AS double_literal,
       array_join(ARRAY[1e2, 3], ',') AS exponent_literal,
       array_join(ARRAY[1, 2], ',') AS all_integers,
       array_join(ARRAY[1.5, 2.5], ',') AS all_decimals

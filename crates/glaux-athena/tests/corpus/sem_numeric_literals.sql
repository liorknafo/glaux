-- Numeric literals: decimals stay exact, exponent literals are doubles, integer arithmetic is checked bigint.
SELECT 1.0 AS one, 0.1 + 0.2 AS exact_sum, CAST(0.1 AS DECIMAL(5, 2)) + 0.2 AS dec_sum, 1e2 AS sci, 2.5E-1 AS sci_frac,
       1.5 * 2 AS dec_times_int, 5 / 2 AS int_div, 7 % 3 AS int_mod, 2 * 3 AS int_mul,
       9223372036854775807 - 1 AS near_max, -1.25 AS neg_dec, 0.5 < 1 AS dec_vs_int,
       sum(x) AS checked_sum, sum(x) - 1 AS checked_difference
FROM (VALUES (4611686018427387903), (4611686018427387904)) AS t(x)

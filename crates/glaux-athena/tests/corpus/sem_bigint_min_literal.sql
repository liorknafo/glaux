-- A unary minus applied to an integer literal is part of the literal (Trino: MINUS? INTEGER_VALUE), so -9223372036854775808 is a bigint, not a decimal.
SELECT -9223372036854775808 AS min_bigint, -2147483648 AS min_integer, -2147483649 AS below_integer, -1 AS minus_one, -(1) AS nested, - 5 AS spaced, 3 -1 AS binary_minus

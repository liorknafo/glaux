-- Integer division truncates towards zero and keeps the wider operand's
-- type, as on Trino; only the type's minimum over -1 overflows (see
-- neg_bigint_division_overflow).
SELECT 7 / 2 AS trunc_pos, -7 / 2 AS trunc_neg, 7 / -2 AS trunc_neg_divisor,
       CAST(9 AS BIGINT) / CAST(2 AS TINYINT) AS widened,
       CAST(-128 AS TINYINT) / CAST(2 AS TINYINT) AS tiny,
       CAST(-9223372036854775808 AS BIGINT) / 2 AS big_min_half,
       id / 2 AS column_div, id / CAST(NULL AS BIGINT) AS null_divisor
FROM customers WHERE id = 5

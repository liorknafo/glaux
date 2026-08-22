-- Decimal arithmetic follows Trino's result types and HALF_UP rounding, which change values, not just text.
SELECT 1.5 / 2 AS half_up, 1.0 / 3 AS third, 10.00 / 3 AS third_2dp, 100 / 3.0 AS int_over_dec,
       CAST(1 AS DECIMAL(38,0)) / 3 AS max_precision_div, -1.5 / 2 AS negative_half_up,
       CAST(amount AS DECIMAL(10,2)) / 3 AS column_div,
       1.5 + 1 AS dec_plus_int, 1.5 * 2 AS dec_times_int, 1.5 - 2.25 AS dec_minus_dec, 1.25 * 1.5 AS dec_times_dec,
       DECIMAL '1.5' AS typed_literal, DECIMAL '1.5' + 1 AS typed_plus_int, DECIMAL '-0.250' AS typed_negative, DECIMAL '15' AS typed_integer,
       DECIMAL '99999999999999999999999999999999999999' AS max_decimal,
       DECIMAL '99999999999999999999999999999999999999' - 1 AS max_decimal_minus_one,
       DECIMAL '9223372036854775808' AS beyond_bigint, 1.5 % 1 AS dec_mod
FROM orders
WHERE id = 101

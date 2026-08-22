-- nullif keeps the first argument's type: Trino coerces only the comparison
-- to the common supertype (nullif(2, 1.0) is integer 2, not decimal 2.0),
-- and a bigint column stays bigint against a decimal probe.
SELECT id, nullif(2, 1.0) AS int_vs_dec, nullif(2, 2.0) AS int_vs_dec_null,
       nullif(CAST(3 AS BIGINT), 2.5) AS bigint_vs_dec,
       nullif(1.5, 2e0) AS dec_vs_double, nullif(1.5, 1.5e0) AS dec_vs_double_null,
       nullif(2.25, 1.0) AS dec_vs_dec, nullif(id, 2.0) AS col_vs_dec
FROM customers WHERE id <= 3 ORDER BY id

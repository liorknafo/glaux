-- Unary minus over the minimum of an integer type overflows; Trino names the type and the value (Arrow said "Overflow happened on: - -128").
-- error: tinyint negation overflow: -128
SELECT -CAST(-128 AS TINYINT)

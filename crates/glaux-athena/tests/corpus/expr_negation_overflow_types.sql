-- Negation is overflow-checked per type, and only the type's minimum overflows (see neg_negate_tinyint_overflow for the failure).
SELECT -CAST(-127 AS TINYINT) AS tiny, -CAST(-32767 AS SMALLINT) AS small,
       -CAST(-2147483647 AS INTEGER) AS int4, -(-9223372036854775807) AS big,
       -CAST(0 AS TINYINT) AS zero, -id AS negated_column
FROM customers WHERE id = 1

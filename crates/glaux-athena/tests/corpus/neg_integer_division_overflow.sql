-- Trino's IntegerOperators.divide has no overflow check and fails later, in
-- IntegerType.writeLong; glaux refuses it loudly with the same code and the
-- bigint path's wording rather than leaking Arrow's message.
-- error: NUMERIC_VALUE_OUT_OF_RANGE: integer division overflow: -2147483648 / -1
SELECT CAST(-2147483648 AS INTEGER) / -1

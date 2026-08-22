-- The same failure away from constant folding: the divisor comes from a
-- column, so the operands are recovered from the batch that overflowed.
-- error: NUMERIC_VALUE_OUT_OF_RANGE: bigint division overflow: -9223372036854775808 / -1
SELECT CAST(-9223372036854775808 AS BIGINT) / -id AS q FROM customers WHERE id = 1

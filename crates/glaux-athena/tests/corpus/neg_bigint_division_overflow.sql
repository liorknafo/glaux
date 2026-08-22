-- The one bigint division that overflows; Trino's BigintOperators.divide
-- words it exactly this way (Arrow said "Overflow happened on: ...").
-- error: NUMERIC_VALUE_OUT_OF_RANGE: bigint division overflow: -9223372036854775808 / -1
SELECT CAST(-9223372036854775808 AS BIGINT) / -1

-- Trino's BigintOperators.add names the operands ("bigint addition
-- overflow: 9223372036854775807 + 1"); Arrow's kernel named neither.
-- error: NUMERIC_VALUE_OUT_OF_RANGE: bigint addition overflow: 9223372036854775807 + 1
SELECT x + 1 FROM (VALUES (9223372036854775807)) AS t(x)

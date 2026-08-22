-- error: NUMERIC_VALUE_OUT_OF_RANGE: bigint addition overflow: 9223372036854775807 + 1
-- Trino's LongSumAggregation accumulates through BigintOperators.add, so
-- the overflow carries that operator's diagnostic — the running total and
-- the value that broke it — where glaux used to say "bigint sum overflow".
SELECT sum(x) FROM (VALUES (9223372036854775807), (1)) AS t(x)

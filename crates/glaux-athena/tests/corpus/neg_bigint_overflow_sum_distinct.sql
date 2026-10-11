-- error: NUMERIC_VALUE_OUT_OF_RANGE: bigint addition overflow
-- sum(DISTINCT x) adds through the same operator.
SELECT sum(DISTINCT x) FROM (VALUES (9223372036854775807), (9223372036854775806)) AS t(x)

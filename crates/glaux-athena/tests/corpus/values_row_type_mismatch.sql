-- error: Values rows have mismatched types: row(bigint) vs row(varchar)
-- Trino refuses a VALUES row list whose rows have no common type; DataFusion
-- alone coerced this into a bigint column with the rows 1, 2.
SELECT x FROM (VALUES (1), ('2')) AS t(x)

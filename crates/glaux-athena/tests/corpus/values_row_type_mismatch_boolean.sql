-- error: Values rows have mismatched types: row(boolean) vs row(bigint)
SELECT x FROM (VALUES (true), (1)) AS t(x)

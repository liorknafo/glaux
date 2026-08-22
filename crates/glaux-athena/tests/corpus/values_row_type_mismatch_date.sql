-- error: Values rows have mismatched types: row(date) vs row(varchar)
-- The pairs DataFusion refuses itself carry Trino's diagnostic too, instead
-- of its raw `Inconsistent data type across values list` text.
SELECT x FROM (VALUES (DATE '2024-01-05'), ('2024-01-06')) AS t(x)

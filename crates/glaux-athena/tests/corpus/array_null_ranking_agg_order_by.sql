-- error: ARRAY comparison not supported for arrays with null elements
-- An aggregate's own ORDER BY is the same ordering as the query-level one.
SELECT array_agg(x ORDER BY x)
FROM (VALUES (ARRAY['a', 'b']), (ARRAY['a', NULL])) AS t(x)

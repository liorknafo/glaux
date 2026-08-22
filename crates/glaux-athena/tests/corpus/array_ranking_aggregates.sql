-- max / min / array_agg(ORDER BY) over arrays without NULL elements.
SELECT max(x) AS max_arr, min(x) AS min_arr, array_agg(x ORDER BY x) AS sorted
FROM (VALUES (ARRAY['a', 'b']), (ARRAY['a', 'c'])) AS t(x)

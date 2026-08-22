-- ORDER BY on arrays without NULL elements uses Trino's lexicographic ordering.
SELECT x FROM (VALUES (ARRAY[1, 5]), (ARRAY[1, 2]), (ARRAY[0, 9]), (ARRAY[1])) t(x) ORDER BY x

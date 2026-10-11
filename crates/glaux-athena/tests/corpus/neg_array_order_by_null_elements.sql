-- error: ARRAY comparison not supported for arrays with null elements
SELECT x FROM (VALUES (ARRAY[1, 5]), (ARRAY[1, NULL])) t(x) ORDER BY x

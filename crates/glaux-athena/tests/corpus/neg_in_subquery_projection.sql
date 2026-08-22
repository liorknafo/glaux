-- error: IN (subquery) as a value
SELECT 3 IN (SELECT x FROM (VALUES (1), (NULL)) t(x))

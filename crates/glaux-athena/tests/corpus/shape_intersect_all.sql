-- INTERSECT ALL keeps the minimum multiplicity of each value (bag semantics), as on Trino.
SELECT x FROM (VALUES (1), (2), (2), (3)) t(x)
INTERSECT ALL
SELECT x FROM (VALUES (2), (2), (2)) t(x)
ORDER BY 1

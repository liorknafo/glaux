-- error: IN (subquery) over DOUBLE / REAL
SELECT x FROM (VALUES (1e0)) t(x) WHERE x IN (SELECT y FROM (VALUES (1e0)) u(y))

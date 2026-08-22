-- Anonymous columns are _colN inside derived tables and VALUES too, and can be referenced by that name.
SELECT a._col0 AS n, a.m, b._col0 AS v1, b._col1 AS v2
FROM (SELECT count(*), max(id) AS m FROM customers) a
CROSS JOIN (VALUES (1, 'x')) b

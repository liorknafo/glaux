-- Join keys of double type use IEEE equality on Trino: the NaN rows never
-- match (DataFusion's hash join would match them); glaux moves the pair
-- into a nested-loop join filter.
SELECT count(*) AS row_count, count(b.y) AS matched
FROM (VALUES (CAST('NaN' AS DOUBLE)), (1e0)) a(x)
LEFT JOIN (VALUES (CAST('NaN' AS DOUBLE)), (1e0)) b(y) ON a.x = b.y

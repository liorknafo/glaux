-- Set operations keep Trino's result types: integer stays integer, integer with decimal is decimal(11,1), decimal with double is double.
SELECT 'int' AS kind, CAST(x AS VARCHAR) AS text FROM (SELECT 1 AS x UNION SELECT 1) t
UNION ALL
SELECT 'dec', CAST(x AS VARCHAR) FROM (SELECT 1 AS x UNION ALL SELECT 1.5) t
UNION ALL
SELECT 'dbl', CAST(x AS VARCHAR) FROM (SELECT 1.5 AS x UNION SELECT 2e0) t
UNION ALL
SELECT 'null', CAST(x AS VARCHAR) FROM (SELECT x FROM (VALUES (1), (NULL)) v(x) INTERSECT SELECT x FROM (VALUES (NULL)) v(x)) t
ORDER BY 1, 2

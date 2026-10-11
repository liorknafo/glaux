-- SELECT DISTINCT over an inline VALUES table with column aliases.
SELECT DISTINCT k, v * 2 AS doubled
FROM (VALUES ('a', 1), ('b', 2), ('a', 1), ('c', NULL)) AS t(k, v)
ORDER BY k

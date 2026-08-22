-- GROUP BY / HAVING resolve to the source column even when an output alias shares its name; ORDER BY may use output aliases.
SELECT status AS s, count(*) AS n, id AS status
FROM orders
GROUP BY status, id
HAVING id > 106
ORDER BY s NULLS FIRST, n

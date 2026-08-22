-- The USING column resolves to coalesce(l.k, r.k) wherever it is referenced; positional ORDER BY follows Trino's column order.
SELECT k, count(*) AS n, max(w) AS w
FROM (VALUES (1, 'x'), (2, 'y')) a(v, k) FULL JOIN (VALUES ('y', 20), ('z', 30)) b(k, w) USING (k)
WHERE k IS NOT NULL
GROUP BY k
HAVING count(*) = 1
ORDER BY 1

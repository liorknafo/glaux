-- SELECT * over USING joins: using columns first, then left, then right; chained joins and nested selects too.
SELECT *
FROM (VALUES (1, 'x')) a(v, k) JOIN (VALUES ('x', 2)) b(k, w) USING (k) JOIN (VALUES (2, 'z')) c(w, z) USING (w)
ORDER BY 1

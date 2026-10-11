-- JOIN ... USING: one coalesced join column, listed first by SELECT *, for every join type.
SELECT 'full' AS kind, * FROM (VALUES (1, 'a'), (2, 'b')) l(k, lv) FULL JOIN (VALUES (2, 'x'), (3, 'y')) r(k, rv) USING (k)
UNION ALL
SELECT 'right', * FROM (VALUES (1, 'a'), (2, 'b')) l(k, lv) RIGHT JOIN (VALUES (2, 'x'), (3, 'y')) r(k, rv) USING (k)
UNION ALL
SELECT 'left', * FROM (VALUES (1, 'a'), (2, 'b')) z(k, lv) LEFT JOIN (VALUES (2, 'x'), (3, 'y')) a(k, rv) USING (k)
UNION ALL
SELECT 'inner', * FROM (VALUES (1, 'a'), (2, 'b')) l(k, lv) JOIN (VALUES (2, 'x'), (3, 'y')) r(k, rv) USING (k)
ORDER BY 1, 2

-- VALUES columns take Trino's common type: integer with NULL stays integer, integer with decimal is decimal(11,1), decimal with double is double.
SELECT * FROM (VALUES (1, 2.5, 1.5, 'a'), (NULL, 3, 2e0, NULL), (2, NULL, NULL, 'c')) t(i, d, f, s) ORDER BY i

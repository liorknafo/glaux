-- The same boundary rule over column data (note = 'a,b,,c'): the empty
-- match right after the leading 'a' is a replacement of its own.
SELECT id, regexp_replace(note, 'a*', 'X') AS replaced
FROM orders
WHERE id = 104

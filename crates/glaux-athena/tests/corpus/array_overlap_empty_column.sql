-- customers.tags row 3 is an empty array and row 5 is NULL: the empty
-- array is false, the NULL array is NULL.
SELECT id, arrays_overlap(tags, ARRAY[NULL]) AS overlaps FROM customers ORDER BY id

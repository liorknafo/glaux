-- error: All ARRAY elements must be the same type or coercible to a common type
-- The element list is refused wherever the array is used, not only when the
-- array itself is the result: `ARRAY['1', 2][1] + 1` used to answer 2.
SELECT ARRAY['1', 2][1] + 1

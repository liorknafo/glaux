-- error: All ARRAY elements must be the same type or coercible to a common type
-- Same for a consumer that stringifies the elements: array_join used to
-- answer '1-2'.
SELECT array_join(ARRAY['1', 2], '-')

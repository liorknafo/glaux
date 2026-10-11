-- error: All ARRAY elements must be the same type or coercible to a common type
-- Trino types ARRAY[...] like every other operand list: mixing varchar with a
-- number is a TYPE_MISMATCH at analysis time, not a coerced `[1, 2]`.
SELECT ARRAY[1, '2']

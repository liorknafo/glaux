-- error: Out of range for integer: 2147483648
-- Arrow said "Can't cast value 2147483648 to type Int32".
SELECT CAST(2147483648 AS INTEGER)

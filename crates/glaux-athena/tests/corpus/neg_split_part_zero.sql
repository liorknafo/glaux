-- error: INVALID_FUNCTION_ARGUMENT: split_part: Index must be greater than zero
-- Trino's StringFunctions.splitPart raises exactly "Index must be greater
-- than zero" (glaux used to append the offending index, which Trino does
-- not).
SELECT split_part('a,b', ',', 0)

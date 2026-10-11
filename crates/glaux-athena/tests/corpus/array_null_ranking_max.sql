-- error: ARRAY comparison not supported for arrays with null elements
-- The `max` aggregate ranks by the same operator as a query-level
-- `ORDER BY`, which already refused these rows; it used to answer `[a, ]`
-- (contradicting `array_max`'s answer on the same two arrays).
SELECT max(tags) FROM customers

-- Trino's `arbitrary(x)` returns "an arbitrary non-null value of x, if one
-- exists": its aggregation state only ever combines non-null values, so a
-- group that starts with a NULL still answers with a value. glaux rewrites
-- it to `first_value(x) IGNORE NULLS`; the plain `first_value` it used to
-- emit respects NULLs, and `arbitrary(amount)` over the last three orders
-- (NULL, 60.0, 10.0) answered NULL where Athena answers 60.0. NULL comes
-- back only when the whole group is NULL, as on Trino.
SELECT arbitrary(amount) AS leading_null,
       arbitrary(status) AS leading_value,
       arbitrary(CAST(NULL AS VARCHAR)) AS all_null
FROM orders
WHERE id >= 106

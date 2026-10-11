-- error: SUBQUERY_MULTIPLE_ROWS: Scalar sub-query has returned multiple rows
SELECT (SELECT id FROM orders)

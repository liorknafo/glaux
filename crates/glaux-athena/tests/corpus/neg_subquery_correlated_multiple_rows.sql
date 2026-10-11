-- error: SUBQUERY_MULTIPLE_ROWS: Scalar sub-query has returned multiple rows
-- An outer row that really does match several inner rows: Trino's runtime
-- error, raised by the guard outside the subquery.
SELECT c.id, (SELECT o.amount FROM orders o WHERE o.customer_id = c.id) FROM customers c

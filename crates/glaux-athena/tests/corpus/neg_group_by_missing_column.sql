-- error: 'orders.id' must be an aggregate expression or appear in GROUP BY clause
-- DataFusion's diagnostic talked about "While expanding wildcard" for a query
-- with no wildcard, and named the grouped column rather than the offending one.
SELECT id FROM orders GROUP BY status

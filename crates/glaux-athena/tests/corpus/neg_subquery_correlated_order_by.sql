-- error: correlated scalar subquery in this clause
-- Trino sorts by a correlated scalar subquery; DataFusion decorrelates one
-- only in SELECT / WHERE / GROUP BY, so it is refused by name instead of
-- leaking "Correlated scalar subquery can only be used in Projection,
-- Filter, Aggregate plan nodes".
SELECT o.id FROM orders o ORDER BY (SELECT c.name FROM customers c WHERE c.id = o.customer_id)

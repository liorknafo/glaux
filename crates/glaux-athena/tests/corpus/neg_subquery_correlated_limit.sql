-- error: correlated scalar subquery is not supported
-- Trino applies the LIMIT per outer row, which decorrelation into a join
-- cannot express; DataFusion left the expression for the physical planner,
-- which dumped the Debug of the logical expression.
SELECT o.id, (SELECT c.name FROM customers c WHERE c.id = o.customer_id LIMIT 1) FROM orders o

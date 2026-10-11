-- error: OFFSET takes a non-negative row count
SELECT id FROM orders LIMIT 1 OFFSET -2

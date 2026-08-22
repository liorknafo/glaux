-- error: EXPRESSION_NOT_SCALAR: WHERE clause cannot contain aggregations, window functions or grouping operations
-- Trino refuses this at analysis; DataFusion planned it and the physical
-- planner dumped the Rust Debug of the window expression.
SELECT id FROM orders WHERE row_number() OVER () = 1

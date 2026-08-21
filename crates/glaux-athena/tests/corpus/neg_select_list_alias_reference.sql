-- error: Column 'x' cannot be resolved
-- Trino does not resolve output aliases inside the select list; DataFusion
-- answered with its schema dump ("No field named x. Valid fields are ...").
SELECT id AS x, x FROM orders LIMIT 1

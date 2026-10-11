-- error: EXPRESSION_NOT_SCALAR: WHERE clause cannot contain aggregations, window functions or grouping operations
-- DataFusion's own wording ("Aggregate functions are not allowed in the
-- WHERE clause. Consider using HAVING instead") carries Trino's sentence.
SELECT id FROM orders WHERE count(*) > 1

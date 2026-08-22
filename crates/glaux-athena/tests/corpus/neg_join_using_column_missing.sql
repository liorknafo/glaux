-- error: Column 'status' cannot be resolved
-- DataFusion added a "Did you mean 'c.tags'?" guess here.
SELECT c.id FROM customers c JOIN orders o USING (status)

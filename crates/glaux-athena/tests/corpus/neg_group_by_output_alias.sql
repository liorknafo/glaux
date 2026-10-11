-- error: Column 's' cannot be resolved
SELECT status s, count(*) FROM orders GROUP BY s

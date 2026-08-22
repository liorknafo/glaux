-- error: Column 'c' cannot be resolved
SELECT status, count(*) c FROM orders GROUP BY status HAVING c > 1

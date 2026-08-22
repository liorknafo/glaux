-- error: EXPRESSION_NOT_SCALAR: HAVING clause cannot contain window functions or grouping operations
SELECT status, count(*) FROM orders GROUP BY status HAVING row_number() OVER () = 1

-- FULL OUTER JOIN keeps orphans on both sides (customer 9 has no row; customers 4/5 have no orders).
SELECT c.id AS customer_id, o.id AS order_id
FROM customers c
FULL OUTER JOIN orders o ON o.customer_id = c.id
WHERE c.id IS NULL OR o.id IS NULL OR c.id > 3
ORDER BY customer_id NULLS LAST, order_id NULLS LAST
